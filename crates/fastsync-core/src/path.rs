use std::path::{Component, Path, PathBuf};

use thiserror::Error;

pub const PARTIAL_FILE_SUFFIX: &str = ".fastsync-part";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PathValidationError {
    #[error("relative path is empty")]
    Empty,
    #[error("absolute paths are not allowed")]
    Absolute,
    #[error("Windows drive paths are not allowed")]
    WindowsDrive,
    #[error("Windows UNC paths are not allowed")]
    WindowsUnc,
    #[error("backslashes are not allowed")]
    Backslash,
    #[error("current-directory components are not allowed")]
    CurrentDirectory,
    #[error("parent-directory components are not allowed")]
    ParentDirectory,
    #[error("empty path components are not allowed")]
    EmptyComponent,
    #[error("NUL bytes are not allowed")]
    Nul,
    #[error("characters unsupported by Windows filenames are not allowed")]
    WindowsInvalidCharacter,
    #[error("Windows filenames cannot end with a dot or space")]
    WindowsTrailingDotOrSpace,
    #[error("Windows reserved device names are not allowed")]
    WindowsReservedName,
    #[error("FastSync staging namespace is reserved")]
    ReservedStagingNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RelativePathError {
    #[error("path is outside the supplied root")]
    OutsideRoot,
    #[error("path contains a non-normal component")]
    NonNormalComponent,
    #[error("path is not valid UTF-8")]
    NonUtf8,
    #[error(transparent)]
    InvalidWirePath(PathValidationError),
}

impl From<PathValidationError> for RelativePathError {
    fn from(error: PathValidationError) -> Self {
        Self::InvalidWirePath(error)
    }
}

pub fn validate_wire_relative_path(path: &str) -> std::result::Result<(), PathValidationError> {
    if path.is_empty() {
        return Err(PathValidationError::Empty);
    }
    if path.contains('\0') {
        return Err(PathValidationError::Nul);
    }
    if path.starts_with("//") || path.starts_with("\\\\") {
        return Err(PathValidationError::WindowsUnc);
    }
    if path.starts_with('/') {
        return Err(PathValidationError::Absolute);
    }
    if path.contains('\\') {
        return Err(PathValidationError::Backslash);
    }

    for component in path.split('/') {
        if component.is_empty() {
            return Err(PathValidationError::EmptyComponent);
        }
        if component == "." {
            return Err(PathValidationError::CurrentDirectory);
        }
        if component == ".." {
            return Err(PathValidationError::ParentDirectory);
        }
        if is_windows_drive_component(component) {
            return Err(PathValidationError::WindowsDrive);
        }
        if component
            .bytes()
            .any(|byte| byte < 32 || matches!(byte, b'<' | b'>' | b':' | b'"' | b'|' | b'?' | b'*'))
        {
            return Err(PathValidationError::WindowsInvalidCharacter);
        }
        if component.ends_with(['.', ' ']) {
            return Err(PathValidationError::WindowsTrailingDotOrSpace);
        }
        if is_windows_reserved_name(component) {
            return Err(PathValidationError::WindowsReservedName);
        }
        if is_fastsync_staging_component(component) {
            return Err(PathValidationError::ReservedStagingNamespace);
        }
    }

    Ok(())
}

/// Validates an untrusted wire path and joins its components to `root`.
///
/// This is a lexical traversal defense. Callers that operate in a directory writable by an
/// untrusted process must separately prevent filesystem symlink races.
pub fn safe_join(
    root: impl AsRef<Path>,
    wire_relative_path: &str,
) -> std::result::Result<PathBuf, PathValidationError> {
    validate_wire_relative_path(wire_relative_path)?;

    let mut joined = root.as_ref().to_path_buf();
    for component in wire_relative_path.split('/') {
        joined.push(component);
    }
    Ok(joined)
}

/// Accepts an absolute path written for either a POSIX or Windows receiver.
pub fn is_receiver_absolute_path(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    if path.is_absolute() {
        return true;
    }
    let Some(raw) = path.to_str() else {
        return false;
    };
    let bytes = raw.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\'))
        || raw.starts_with("\\\\")
        || raw.starts_with("//")
}

pub fn is_fastsync_staging_component(component: &str) -> bool {
    const PREFIX: &[u8] = b".fastsync-stage-";
    component
        .as_bytes()
        .get(..PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
}

/// Converts a path below `root` to FastSync's UTF-8, forward-slash wire representation.
pub fn to_wire_relative_path(
    root: impl AsRef<Path>,
    path: impl AsRef<Path>,
) -> std::result::Result<String, RelativePathError> {
    let relative = path
        .as_ref()
        .strip_prefix(root.as_ref())
        .map_err(|_| RelativePathError::OutsideRoot)?;

    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_str().ok_or(RelativePathError::NonUtf8)?;
                components.push(value);
            }
            _ => return Err(RelativePathError::NonNormalComponent),
        }
    }

    let wire_path = components.join("/");
    validate_wire_relative_path(&wire_path)?;
    Ok(wire_path)
}

/// Returns the sibling staging path used while transferring `destination`.
pub fn partial_file_path(destination: impl AsRef<Path>) -> PathBuf {
    let mut partial = destination.as_ref().as_os_str().to_os_string();
    partial.push(PARTIAL_FILE_SUFFIX);
    PathBuf::from(partial)
}

fn is_windows_drive_component(component: &str) -> bool {
    let bytes = component.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or(component);
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}
