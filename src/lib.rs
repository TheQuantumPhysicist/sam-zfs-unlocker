use std::collections::BTreeMap;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

#[derive(thiserror::Error, Debug)]
pub enum ZfsError {
    #[error("System error: {0}")]
    SystemError(String),
    #[error("Dataset {0} not found")]
    DatasetNotFound(String),
    #[error("Command returned unexpected state for key is available, other than 'available' and 'unavailable': {0}")]
    UnexpectedStateForKey(String),
    #[error("Command returned unexpected state for mount, other than 'yes' and 'no': {0}")]
    UnexpectedStateForMount(String),
    #[error("Command to check whether dataset {0} is mounted failed: {1}")]
    IsMountedCheckCallFailed(String, String),
    #[error("Command to list datasets mount points failed: {0}")]
    ListDatasetsMountPointsCallFailed(String),
    #[error("Command to list unmounted datasets failed: {0}")]
    ListUnmountedDatasetsCallFailed(String),
    #[error("Command to check whether key for dataset {0} is loaded failed: {1}")]
    KeyLoadedCheckFailed(String, String),
    #[error("Load key command for dataset {0} failed: {1}")]
    LoadKeyCmdFailed(String, String),
    #[error("Unload key command for dataset {0} failed: {1}")]
    UnloadKeyCmdFailed(String, String),
    #[error("Key must be loaded before mount for dataset {0}")]
    KeyNotLoadedForMount(String),
    #[error("Mount command for dataset {0} failed: {1}")]
    MountCmdFailed(String, String),
    #[error("Unmount command for dataset {0} failed: {1}")]
    UnmountCmdFailed(String, String),
    #[error("Dataset name is invalid: {0}")]
    DatasetNameIsInvalid(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetMountedState {
    pub dataset_name: String,
    pub is_mounted: bool,
    pub is_key_loaded: bool,
}

fn parse_key_available_state(state: impl AsRef<str>) -> Result<bool, ZfsError> {
    match state.as_ref().trim() {
        "available" => Ok(true),
        "unavailable" => Ok(false),
        _ => Err(ZfsError::UnexpectedStateForKey(state.as_ref().to_string())),
    }
}

fn parse_dataset_mounted_state(state: impl AsRef<str>) -> Result<bool, ZfsError> {
    match state.as_ref().trim() {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(ZfsError::UnexpectedStateForMount(
            state.as_ref().to_string(),
        )),
    }
}

/// Note that the sanitization's purpose is not to perfectly mimic ZFS specs.
/// The purpose is to prevent any kind of possible injection of commands.
fn check_and_sanitize_zfs_dataset_name(zfs_dataset: impl AsRef<str>) -> Result<String, ZfsError> {
    const ALLOWED_SYMBOLS: [char; 4] = ['-', '_', '.', ':'];

    let dataset = zfs_dataset.as_ref().trim();

    let check_func = |part: &str| {
        part.chars()
            .all(|c| c.is_ascii_alphanumeric() || ALLOWED_SYMBOLS.contains(&c))
            && part.chars().all(|c| !c.is_whitespace())
            && !part.is_empty()
            && !part.starts_with(&ALLOWED_SYMBOLS) // Can only begin with an alphanumeric
    };

    // Check the whole name, then the individual parts
    check_func(dataset);

    if !dataset.split('/').all(|part| check_func(part)) {
        Err(ZfsError::DatasetNameIsInvalid(dataset.to_string()))
    } else {
        Ok(dataset.to_string())
    }
}

/// Attempts to load-key for ZFS dataset
/// Returns: Ok(()) if the key is successfully loaded OR already loaded
/// Returns: Error if dataset not found or some other system error occurred.
/// The command `zfs load-key <dataset-name>` should be authorized with visudo.
pub fn zfs_load_key(
    zfs_dataset: impl AsRef<str>,
    passphrase: impl AsRef<str>,
) -> Result<(), ZfsError> {
    let passphrase = passphrase.as_ref();
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

    match zfs_is_key_loaded(&dataset)? {
        Some(loaded) => match loaded {
            true => return Ok(()),
            false => (),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("load-key")
        .arg(&dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::LoadKeyCmdFailed(dataset.to_string(), e.to_string()))?;

    // Get the stdin of the zfs command
    if let Some(mut stdin) = child.stdin.take() {
        // Write the key to stdin
        let mut writer = BufWriter::new(&mut stdin);
        writeln!(writer, "{}", passphrase).map_err(|e| ZfsError::SystemError(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    }

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::LoadKeyCmdFailed(
            dataset.to_string(),
            stderr_string,
        ))
    }
}

/// Attempts to load-key for ZFS dataset
/// Returns: Ok(()) if the key is successfully unloaded OR already unloaded
/// Returns: Error if dataset not found or some other system error occurred.
/// The command `zfs unload-key <dataset-name>` should be authorized with visudo.
pub fn zfs_unload_key(zfs_dataset: impl AsRef<str>) -> Result<(), ZfsError> {
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

    match zfs_is_key_loaded(&dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => return Ok(()),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("unload-key")
        .arg(&dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::UnloadKeyCmdFailed(dataset.to_string(), e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::UnloadKeyCmdFailed(
            dataset.to_string(),
            stderr_string,
        ))
    }
}

/// Mounts a ZFS dataset
/// Returns Ok(()) if successfully mounted or already mounted
/// Returns Err otherwise
/// The command `zfs mount <dataset-name>` should be authorized with visudo.
pub fn zfs_mount_dataset(zfs_dataset: impl AsRef<str>) -> Result<(), ZfsError> {
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

    match zfs_is_key_loaded(&dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => return Err(ZfsError::KeyNotLoadedForMount(dataset.to_string())),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    match zfs_is_dataset_mounted(&dataset)? {
        Some(mounted) => match mounted {
            true => return Ok(()),
            false => (),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("mount")
        .arg(&dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::MountCmdFailed(dataset.to_string(), e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::MountCmdFailed(dataset.to_string(), stderr_string))
    }
}

/// Unmounts a ZFS dataset
/// Returns: Ok(()) on success or if is already mounted
/// Returns: Err otherwise.
/// The command `zfs unmount <dataset-name>` should be authorized with visudo.
pub fn zfs_unmount_dataset(zfs_dataset: impl AsRef<str>) -> Result<(), ZfsError> {
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

    match zfs_is_dataset_mounted(&dataset)? {
        Some(mounted) => match mounted {
            true => (),
            false => return Ok(()),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    // Create a command to run zfs load-key
    let mut child = Command::new("sudo")
        .arg("-n") // sudo isn't interactive
        .arg("zfs")
        .arg("umount")
        .arg(&dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::UnmountCmdFailed(dataset.to_string(), e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::UnmountCmdFailed(
            dataset.to_string(),
            stderr_string,
        ))
    }
}

/// Checks whether key is loaded
/// Returns: Some(true): Key is available/loaded and/or doesn't need it
/// Returns: Some(false): Key is not loaded
/// Returns: None: The dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_key_loaded(zfs_dataset: impl AsRef<str>) -> Result<Option<bool>, ZfsError> {
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

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
        .spawn()
        .map_err(|e| ZfsError::KeyLoadedCheckFailed(dataset.to_string(), e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0], v[1]))
            .collect::<BTreeMap<&str, &str>>();
        match datasets_results.get(&*dataset) {
            Some(is_key_available) => parse_key_available_state(is_key_available).map(Some),
            None => Ok(None),
        }
    } else {
        Err(ZfsError::KeyLoadedCheckFailed(
            dataset.to_string(),
            stderr_string,
        ))
    }
}

/// Checks whether a dataset is mounted
/// Returns: Some(true): The dataset is mounted
/// Returns: Some(false): The dataset is not mounted
/// Returns: None: The dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_dataset_mounted(zfs_dataset: impl AsRef<str>) -> Result<Option<bool>, ZfsError> {
    let dataset = check_and_sanitize_zfs_dataset_name(zfs_dataset)?;

    // Create a command to run zfs load-key
    let mut child = Command::new("zfs")
        .arg("list")
        .arg("-H") // No table header
        .arg("-o")
        .arg("name,mounted") // Only show two columns, dataset name and whether dataset is mounted
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::IsMountedCheckCallFailed(dataset.to_string(), e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0], v[1]))
            .collect::<BTreeMap<&str, &str>>();
        match datasets_results.get(&*dataset) {
            Some(is_dataset_mounted) => match *is_dataset_mounted {
                "yes" => Ok(Some(true)),
                "no" => Ok(Some(false)),
                _ => Err(ZfsError::UnexpectedStateForMount(
                    is_dataset_mounted.to_string(),
                )),
            },
            None => Ok(None),
        }
    } else {
        Err(ZfsError::IsMountedCheckCallFailed(
            dataset.to_string(),
            stderr_string,
        ))
    }
}

pub fn zfs_list_datasets_mountpoints() -> Result<BTreeMap<String, PathBuf>, ZfsError> {
    // Create a command to run zfs load-key
    let mut child = Command::new("zfs")
        .arg("list")
        .arg("-H") // No table header
        .arg("-o")
        .arg("name,mountpoint") // Only show two columns, dataset name and mountpoint
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::ListDatasetsMountPointsCallFailed(e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0].to_string(), PathBuf::from(v[1])))
            .collect::<BTreeMap<String, PathBuf>>();
        Ok(datasets_results)
    } else {
        Err(ZfsError::ListDatasetsMountPointsCallFailed(stderr_string))
    }
}

pub fn zfs_list_encrypted_datasets() -> Result<BTreeMap<String, DatasetMountedState>, ZfsError> {
    // Create a command to run zfs load-key
    let mut child = Command::new("zfs")
        .arg("list")
        .arg("-H") // No table header
        .arg("-o")
        .arg("name,mounted,keystatus") // Only show two columns, dataset name and mountpoint
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::ListDatasetsMountPointsCallFailed(e.to_string()))?;

    // Capture the stdout handle of the child process
    let mut stdout = child.stdout.take().expect("Failed to capture stdout");
    let mut stderr = child.stderr.take().expect("Failed to capture stderr");

    // Read stdout/stderr to a string
    let mut stdout_string = String::new();
    stdout
        .read_to_string(&mut stdout_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;
    let mut stderr_string = String::new();
    stderr
        .read_to_string(&mut stderr_string)
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Wait for the zfs command to complete
    let status = child
        .wait()
        .map_err(|e| ZfsError::SystemError(e.to_string()))?;

    // Check if the command was successful
    if status.success() {
        let lines = stdout_string.lines();
        let datasets_results = lines
            .into_iter()
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .filter(|v| v.len() >= 3)
            .filter(|v| v[2].trim() != "-") // Filter unencrypted datasets
            .map(|v| {
                let dataset_name = v[0].to_string();
                let is_mounted = parse_dataset_mounted_state(v[1])?;
                let is_key_loaded = parse_key_available_state(v[2])?;
                Ok((
                    dataset_name.clone(),
                    DatasetMountedState {
                        dataset_name,
                        is_mounted,
                        is_key_loaded,
                    },
                ))
            })
            .collect::<Result<BTreeMap<String, DatasetMountedState>, _>>()?;
        Ok(datasets_results)
    } else {
        Err(ZfsError::ListUnmountedDatasetsCallFailed(stderr_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        // Feel free to update these entries to your machine's entries to test
        let hostname = "pitests";
        let ds_name = "SamRandomPool/EncryptedDataset1";
        let passphrase = "abcdefghijklmnop";
        let mount_point = "/SamRandomPoolEncryptedDS1";

        if hostname::get().unwrap().to_string_lossy().to_lowercase() == hostname.to_lowercase() {
            // Try with a non-existent database
            assert_eq!(zfs_is_key_loaded("some_random_stuff").unwrap(), None);

            // Unmount, before messing with the key
            zfs_unmount_dataset(ds_name).unwrap();

            // Ensure the key is unloaded and db is unmounted, load it, then unload it
            zfs_unload_key(ds_name).unwrap();
            assert_eq!(zfs_is_key_loaded(ds_name).unwrap(), Some(false));
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_key_loaded,
                false
            );
            zfs_load_key(ds_name, passphrase).unwrap();
            assert_eq!(zfs_is_key_loaded(ds_name).unwrap(), Some(true));
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_key_loaded,
                true
            );
            zfs_unload_key(ds_name).unwrap();
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_key_loaded,
                false
            );
            assert_eq!(zfs_is_key_loaded(ds_name).unwrap(), Some(false));

            zfs_load_key(ds_name, passphrase).unwrap();
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_key_loaded,
                true
            );
            assert_eq!(zfs_is_key_loaded(ds_name).unwrap(), Some(true));

            zfs_unmount_dataset(ds_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(ds_name).unwrap(), Some(false));
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_mounted,
                false
            );
            zfs_mount_dataset(ds_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(ds_name).unwrap(), Some(true));
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_mounted,
                true
            );
            zfs_unmount_dataset(ds_name).unwrap();
            assert_eq!(zfs_is_dataset_mounted(ds_name).unwrap(), Some(false));
            assert_eq!(
                zfs_list_encrypted_datasets()
                    .unwrap()
                    .get(ds_name)
                    .unwrap()
                    .is_mounted,
                false
            );

            zfs_unload_key(ds_name).unwrap();
            assert_eq!(zfs_is_key_loaded(ds_name).unwrap(), Some(false));

            let mount_points = zfs_list_datasets_mountpoints().unwrap();
            assert_eq!(
                mount_points.get(ds_name).unwrap().to_string_lossy(),
                mount_point,
            );
        } else {
            let err = "WARNING: No tests were run. Update the tests to test on your machine.";
            println!("{}", err);
            eprintln!("{}", err);
        }
    }

    #[test]
    fn test_valid_zfs_dataset_names() {
        let f = check_and_sanitize_zfs_dataset_name;

        f("pool/dataset1").unwrap();
        f("pool/dataset_2").unwrap();
        f("pool.dataset/dataset-3").unwrap();
        f("pool.dataset/dataset:3").unwrap();
        f("pool:1/dataset.with.multiple.levels").unwrap();
        f(" pool:1/dataset.with.multiple.levels").unwrap();
        f(" pool:1/dataset.with.multiple.levels  ").unwrap();
    }

    #[test]
    fn test_invalid_zfs_dataset_names() {
        let f = check_and_sanitize_zfs_dataset_name;

        f("").unwrap_err();
        f("_R").unwrap_err();
        f("-R").unwrap_err();
        f(":R").unwrap_err();
        f(".R").unwrap_err();
        f(" _R").unwrap_err();
        f(" -R").unwrap_err();
        f(" :R").unwrap_err();
        f(" .R").unwrap_err();
        f("pool/_R").unwrap_err();
        f("pool/-R").unwrap_err();
        f("pool/:R").unwrap_err();
        f("pool/.R").unwrap_err();
        f("pool/ _R").unwrap_err();
        f("pool/ -R").unwrap_err();
        f("pool/ :R").unwrap_err();
        f("pool/ .R").unwrap_err();
        f("pool/dataset name").unwrap_err();
        f("pool/dataset!").unwrap_err();
        f("pool/dataset@name").unwrap_err();
        f("pool//dataset").unwrap_err();
        f("pool/ dataset").unwrap_err();
    }

    #[test]
    fn key_loaded_state() {
        assert_eq!(parse_key_available_state("available").unwrap(), true);
        assert_eq!(parse_key_available_state("unavailable").unwrap(), false);
        assert_eq!(parse_key_available_state(" available").unwrap(), true);
        assert_eq!(parse_key_available_state(" unavailable").unwrap(), false);
        assert_eq!(parse_key_available_state("available ").unwrap(), true);
        assert_eq!(parse_key_available_state("unavailable ").unwrap(), false);
        assert_eq!(parse_key_available_state(" available ").unwrap(), true);
        assert_eq!(parse_key_available_state(" unavailable ").unwrap(), false);

        parse_key_available_state("yes").unwrap_err();
        parse_key_available_state("no").unwrap_err();
        parse_key_available_state(" ").unwrap_err();
        parse_key_available_state(".").unwrap_err();
        parse_key_available_state("2222").unwrap_err();
    }

    #[test]
    fn is_mounted_state() {
        assert_eq!(parse_dataset_mounted_state("yes").unwrap(), true);
        assert_eq!(parse_dataset_mounted_state("no").unwrap(), false);
        assert_eq!(parse_dataset_mounted_state(" yes").unwrap(), true);
        assert_eq!(parse_dataset_mounted_state(" no").unwrap(), false);
        assert_eq!(parse_dataset_mounted_state("yes ").unwrap(), true);
        assert_eq!(parse_dataset_mounted_state("no ").unwrap(), false);
        assert_eq!(parse_dataset_mounted_state(" yes ").unwrap(), true);
        assert_eq!(parse_dataset_mounted_state(" no ").unwrap(), false);

        parse_dataset_mounted_state("available").unwrap_err();
        parse_dataset_mounted_state("unavailable").unwrap_err();
        parse_dataset_mounted_state(" ").unwrap_err();
        parse_dataset_mounted_state(".").unwrap_err();
        parse_dataset_mounted_state("2222").unwrap_err();
    }
}
