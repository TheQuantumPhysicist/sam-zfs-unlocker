use std::collections::BTreeMap;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::process::Command;

/// Attempts to load-key for ZFS dataset
/// Returns: Ok(()) if the key is successfully loaded OR already loaded
/// Returns: Error if dataset not found or some other system error occurred.
/// The command `zfs load-key <dataset-name>` should be authorized with visudo.
pub fn zfs_load_key(
    zfs_dataset: impl AsRef<str>,
    passphrase: impl AsRef<str>,
) -> anyhow::Result<()> {
    let passphrase = passphrase.as_ref();
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
        Some(loaded) => match loaded {
            true => return Ok(()),
            false => (),
        },
        None => return Err(anyhow::anyhow!("ZFS dataset {dataset} not found.")),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("load-key")
        .arg(dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Get the stdin of the zfs command
    if let Some(mut stdin) = child.stdin.as_mut() {
        // Write the key to stdin
        let mut writer = BufWriter::new(&mut stdin);
        writeln!(writer, "{}", passphrase)?;
        writer.flush()?;
    }

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Failed to load ZFS key. Error: {}",
            stderr_string
        ))
    }
}

/// Attempts to load-key for ZFS dataset
/// Returns: Ok(()) if the key is successfully unloaded OR already unloaded
/// Returns: Error if dataset not found or some other system error occurred.
/// The command `zfs unload-key <dataset-name>` should be authorized with visudo.
pub fn zfs_unload_key(zfs_dataset: impl AsRef<str>) -> anyhow::Result<()> {
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => return Ok(()),
        },
        None => return Err(anyhow::anyhow!("ZFS dataset {dataset} not found.")),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("unload-key")
        .arg(dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Failed to unload ZFS key. Error: {}",
            stderr_string
        ))
    }
}

/// Mounts a ZFS dataset
/// Returns Ok(()) if successfully mounted or already mounted
/// Returns Err otherwise
/// The command `zfs mount <dataset-name>` should be authorized with visudo.
pub fn zfs_mount_dataset(zfs_dataset: impl AsRef<str>) -> anyhow::Result<()> {
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => {
                return Err(anyhow::anyhow!(
                    "Cannot mount encrypted dataset. Key not loaded."
                ))
            }
        },
        None => {
            return Err(anyhow::anyhow!(
                "ZFS dataset {dataset} not found  [when checking key-loaded]."
            ))
        }
    }

    match zfs_is_dataset_mounted(dataset)? {
        Some(mounted) => match mounted {
            true => return Ok(()),
            false => (),
        },
        None => {
            return Err(anyhow::anyhow!(
                "ZFS dataset {dataset} not found [when checking mounted]."
            ))
        }
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("mount")
        .arg(dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Failed to mount ZFS dataset. Error: {}",
            stderr_string
        ))
    }
}

/// Unmounts a ZFS dataset
/// Returns: Ok(()) on success or if is already mounted
/// Returns: Err otherwise.
/// The command `zfs unmount <dataset-name>` should be authorized with visudo.
pub fn zfs_unmount_dataset(zfs_dataset: impl AsRef<str>) -> anyhow::Result<()> {
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => {
                return Err(anyhow::anyhow!(
                    "Cannot mount encrypted dataset. Key not loaded."
                ))
            }
        },
        None => {
            return Err(anyhow::anyhow!(
                "ZFS dataset {dataset} not found  [when checking key-loaded]."
            ))
        }
    }

    match zfs_is_dataset_mounted(dataset)? {
        Some(mounted) => match mounted {
            true => (),
            false => return Ok(()),
        },
        None => {
            return Err(anyhow::anyhow!(
                "ZFS dataset {dataset} not found [when checking mounted]."
            ))
        }
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("unmount")
        .arg(dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Failed to mount ZFS dataset. Error: {}",
            stderr_string
        ))
    }
}

/// Checks whether key is loaded
/// Returns: Some(true) if key is available/loaded and/or doesn't need it
/// Returns: Some(false) if key is not loaded
/// Returns: None if the dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_key_loaded(zfs_dataset: impl AsRef<str>) -> anyhow::Result<Option<bool>> {
    let dataset = zfs_dataset.as_ref();

    // Create a command to run zfs load-key
    let mut child = Command::new("zfs")
        .arg("get")
        .arg("keystatus")
        .arg("-H") // No table header
        .arg("-o")
        .arg("name,value") // Only show two columns, dataset name and whether key is available
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0], v[1]))
            .collect::<BTreeMap<&str, &str>>();
        match datasets_results.get(dataset) {
            Some(is_key_available) => match *is_key_available {
                "available" => Ok(Some(true)),
                "unavailable" => Ok(Some(false)),
                "-" => Ok(Some(true)),
                _ => Err(anyhow::anyhow!(
                    "Unknown result returned by ZFS dataset query for key status: `{}`",
                    is_key_available
                )),
            },
            None => Ok(None),
        }
    } else {
        Err(anyhow::anyhow!(
            "Failed to check ZFS dataset status: {}",
            stderr_string
        ))
    }
}

/// Checks whether a dataset is mounted
/// Returns: Some(true) if key is available/loaded and/or doesn't need it
/// Returns: Some(false) if key is not loaded
/// Returns: None if the dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_dataset_mounted(zfs_dataset: impl AsRef<str>) -> anyhow::Result<Option<bool>> {
    let dataset = zfs_dataset.as_ref();

    // Create a command to run zfs load-key
    let mut child = Command::new("zfs")
        .arg("list")
        .arg("-H") // No table header
        .arg("-o")
        .arg("name,mounted") // Only show two columns, dataset name and whether key is available
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout.read_to_string(&mut stdout_string)?;
    let mut stderr_string = String::new();
    stderr.read_to_string(&mut stderr_string)?;

    // Wait for the zfs command to complete
    let status = child.wait()?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0], v[1]))
            .collect::<BTreeMap<&str, &str>>();
        match datasets_results.get(dataset) {
            Some(is_key_available) => match *is_key_available {
                "yes" => Ok(Some(true)),
                "no" => Ok(Some(false)),
                _ => Err(anyhow::anyhow!(
                    "Unknown result returned by ZFS dataset query for mounted status: `{}`",
                    is_key_available
                )),
            },
            None => Ok(None),
        }
    } else {
        Err(anyhow::anyhow!(
            "Failed to check ZFS dataset mounted status: {}",
            stderr_string
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        zfs_is_dataset_mounted, zfs_is_key_loaded, zfs_load_key, zfs_mount_dataset, zfs_unload_key,
        zfs_unmount_dataset,
    };

    #[test]
    fn basic() {
        let hostname = hostname::get().unwrap();

        let dataset_name = "SamRandomPool/EncryptedDataset1";
        let passphrase = "abcdefghijklmnop";

        if hostname.to_ascii_lowercase() == "pitests" {
            // Try with a non-existent database
            assert_eq!(zfs_is_key_loaded("some_random_stuff").unwrap(), None);

            // Ensure the key is unloaded, load it, then unload it
            zfs_unload_key(dataset_name).unwrap();
            assert_eq!(zfs_is_key_loaded(dataset_name).unwrap(), Some(false));
            zfs_load_key(dataset_name, passphrase).unwrap();
            assert_eq!(zfs_is_key_loaded(dataset_name).unwrap(), Some(true));
            zfs_unload_key(dataset_name).unwrap();
            assert_eq!(zfs_is_key_loaded(dataset_name).unwrap(), Some(false));

            zfs_load_key(dataset_name, passphrase).unwrap();
            assert_eq!(zfs_is_key_loaded(dataset_name).unwrap(), Some(true));

            zfs_unmount_dataset(dataset_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(dataset_name).unwrap(), Some(false));
            zfs_mount_dataset(dataset_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(dataset_name).unwrap(), Some(true));
            zfs_unmount_dataset(dataset_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(dataset_name).unwrap(), Some(false));

            zfs_unload_key(dataset_name).unwrap();
            assert_eq!(zfs_is_key_loaded(dataset_name).unwrap(), Some(false));
        } else {
            println!("WARNING: No tests were run. Hostname not known.");
        }
    }
}
