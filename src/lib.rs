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
    #[error("Command returned unexpected state for mount, other than 'yes' and 'no': {0}")]
    UnexpectedStateForMount(String),
    #[error("Command to check whether dataset {0} is mounted failed: {1}")]
    IsMountedCheckCallFailed(String, String),
    #[error("Command to list datasets mount points failed: {0}")]
    ListDatasetsMountPointsCallFailed(String),
    #[error(
        "Command returned unexpected state for key-loaded, other than 'true' and 'false' and '-'"
    )]
    UnexpectedStateForKeyLoaded(String),
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
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
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
        .arg(dataset)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ZfsError::LoadKeyCmdFailed(dataset.to_string(), e.to_string()))?;

    // Get the stdin of the zfs command
    if let Some(mut stdin) = child.stdin.as_mut() {
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
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
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
        .arg(dataset)
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
    let dataset = zfs_dataset.as_ref();

    match zfs_is_key_loaded(dataset)? {
        Some(loaded) => match loaded {
            true => (),
            false => return Err(ZfsError::KeyNotLoadedForMount(dataset.to_string())),
        },
        None => return Err(ZfsError::DatasetNotFound(dataset.to_string())),
    }

    match zfs_is_dataset_mounted(dataset)? {
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
        .arg(dataset)
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
    let dataset = zfs_dataset.as_ref();

    match zfs_is_dataset_mounted(dataset)? {
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
        .arg(dataset)
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
/// Returns: Some(true) if key is available/loaded and/or doesn't need it
/// Returns: Some(false) if key is not loaded
/// Returns: None if the dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_key_loaded(zfs_dataset: impl AsRef<str>) -> Result<Option<bool>, ZfsError> {
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
        match datasets_results.get(dataset) {
            Some(is_key_available) => match *is_key_available {
                "available" => Ok(Some(true)),
                "unavailable" => Ok(Some(false)),
                "-" => Ok(Some(true)),
                _ => Err(ZfsError::UnexpectedStateForKeyLoaded(
                    is_key_available.to_string(),
                )),
            },
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
/// Returns: Some(true) if key is available/loaded and/or doesn't need it
/// Returns: Some(false) if key is not loaded
/// Returns: None if the dataset is not found
/// Otherwise, an error is returned
pub fn zfs_is_dataset_mounted(zfs_dataset: impl AsRef<str>) -> Result<Option<bool>, ZfsError> {
    let dataset = zfs_dataset.as_ref();

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
        match datasets_results.get(dataset) {
            Some(is_key_available) => match *is_key_available {
                "yes" => Ok(Some(true)),
                "no" => Ok(Some(false)),
                _ => Err(ZfsError::UnexpectedStateForMount(
                    is_key_available.to_string(),
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

#[cfg(test)]
mod tests {
    use crate::{
        zfs_is_dataset_mounted, zfs_is_key_loaded, zfs_list_datasets_mountpoints, zfs_load_key,
        zfs_mount_dataset, zfs_unload_key, zfs_unmount_dataset,
    };

    #[test]
    fn basic() {
        // Feel free to update these entries to your machine's entries to test
        let hostname = "pitests";
        let dataset_name = "SamRandomPool/EncryptedDataset1";
        let passphrase = "abcdefghijklmnop";
        let mount_point = "/SamRandomPoolEncryptedDS1";

        if hostname::get().unwrap() == hostname {
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

            let mount_points = zfs_list_datasets_mountpoints().unwrap();
            assert_eq!(
                mount_points.get(dataset_name).unwrap().to_string_lossy(),
                mount_point,
            );
        } else {
            let err = "WARNING: No tests were run. Update the tests to test on your machine.";
            println!("{}", err);
            eprintln!("{}", err);
        }
    }
}
