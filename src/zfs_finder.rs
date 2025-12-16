use crate::ZfsError;
use std::{
    collections::BTreeSet,
    process::{Command, Stdio},
};

/// Returns all absolute paths for `bin` visible in the current process environment (PATH),
/// in a deterministic (sorted) order.
///
/// Implementation notes:
/// - Uses `sh -lc "command -v -a -- <bin>"` to emulate typical shell path resolution.
/// - Suppresses shell errors and returns an empty set if the command fails.
/// - Intended for diagnostics / path selection, not for security decisions.
fn which_all(bin: &str) -> Result<BTreeSet<String>, ZfsError> {
    let out = Command::new("sh")
        .args([
            "-lc",
            &format!("command -v -a -- {bin} 2>/dev/null || true"),
        ])
        .output()
        .map_err(|e| ZfsError::WhichAllFailed(bin.to_string(), e.to_string()))?;

    let stdout = String::from_utf8_lossy(&out.stdout);
    Ok(stdout
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

/// Runs `sudo -n -l` and returns combined stdout+stderr.
///
/// Purpose:
/// - Capture what *this exact process context* is allowed to run without a password.
/// - This is sensitive to sudoers, user, host, and sudo Defaults (notably `secure_path`).
///
/// Failure behavior:
/// - Returns an empty string if sudo cannot be executed or the output can’t be collected.
/// - With `-n`, sudo will not prompt; failures are reported in stderr (captured here).
fn sudo_l_output() -> Result<String, ZfsError> {
    let out = Command::new("sudo")
        .args(["-n", "-l"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| ZfsError::SudoListFailed(e.to_string()))?;

    Ok(format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    ))
}

/// Checks whether the `sudo -l` output contains an exact textual rule substring.
///
/// This is intentionally a simple substring match, because `sudo -l` is free-form text.
/// If you need stronger parsing, use `sudo -l -l` (long format) and parse carefully.
fn sudo_allows_exact_from_output(sudo_l: &str, rule: &str) -> bool {
    sudo_l.contains(rule)
}

/// Extract a single canonical `/.../zfs` path from `sudo -n -l`, requiring it to be unique.
///
/// This avoids threading `sudo_l` through callers (less error-prone), and validates the
/// extracted token by checking it appears as part of a sudoers command spec.
fn sudoers_single_zfs_path() -> Result<String, ZfsError> {
    let sudo_l = sudo_l_output()?;

    let mut found: BTreeSet<String> = BTreeSet::new();

    for chunk in sudo_l.split(',') {
        let c = chunk.trim();

        if let Some(pos) = c.find("/zfs") {
            let start = c[..pos]
                .rfind(|ch: char| ch.is_whitespace() || ch == ':')
                .map(|i| i + 1)
                .unwrap_or(0);

            let rest = &c[start..];
            let end = rest
                .find(|ch: char| ch.is_whitespace())
                .unwrap_or(rest.len());
            let token = &rest[..end];

            if token.ends_with("/zfs") {
                // Validate that this token is actually in the sudoers spec text.
                // (We can't "use" sudo_allows_exact... to *find* the token, but we can
                // use it to sanity-check what we found.)
                if sudo_allows_exact_from_output(&sudo_l, token) {
                    found.insert(token.to_string());
                }
            }
        }
    }

    match found.len() {
        1 => Ok(found.into_iter().next().unwrap()),
        0 => Err(ZfsError::SudoersNoMatchingRule(
            "No unique /.../zfs entry found in `sudo -n -l` output".to_string(),
        )),
        _ => Err(ZfsError::SudoersNoMatchingRule(format!(
            "Multiple zfs paths found in sudoers (`sudo -n -l`): {:?}",
            found.into_iter().collect::<Vec<_>>()
        ))),
    }
}

/// Chooses the `zfs` executable path to use with `sudo`, based on what sudoers
/// *actually* allows in this runtime context.
///
/// Algorithm:
/// 1) Read `sudo -n -l` and try to extract exactly one canonical `/.../zfs` path.
/// 2) Verify that this path exists in either common locations or the process PATH
///    (helps catch typos / stale sudoers).
/// 3) If sudoers doesn't expose a unique zfs path, fall back to PATH only if exactly
///    one zfs is found; otherwise error.
///
/// This avoids hard-coding subcommands like `load-key` and assumes the admin uses a
/// single zfs binary path across their rules.
pub fn pick_zfs_path_for_sudo() -> Result<String, ZfsError> {
    // Candidate paths we consider "realistic".
    let mut candidates: BTreeSet<String> = BTreeSet::new();
    candidates.insert("/usr/sbin/zfs".to_string());
    candidates.insert("/usr/bin/zfs".to_string());
    candidates.extend(which_all("zfs")?);

    // Prefer what sudoers itself implies.
    if let Ok(zfs_path) = sudoers_single_zfs_path() {
        if candidates.contains(&zfs_path) {
            return Ok(zfs_path);
        }
        return Err(ZfsError::SudoersNoMatchingRule(format!(
            "sudoers references zfs at `{zfs_path}`, but it is not discoverable (candidates: {:?})",
            candidates.into_iter().collect::<Vec<_>>()
        )));
    }

    // Fallback: if sudoers doesn't clearly show a zfs path, require PATH to be unambiguous.
    let from_path = which_all("zfs")?;
    match from_path.len() {
        1 => Ok(from_path.into_iter().next().unwrap()),
        0 => Err(ZfsError::ZfsNotFoundInPath),
        _ => Err(ZfsError::SudoersNoMatchingRule(format!(
            "sudoers did not expose a unique zfs path, and PATH has multiple zfs entries: {:?}",
            from_path.into_iter().collect::<Vec<_>>()
        ))),
    }
}
