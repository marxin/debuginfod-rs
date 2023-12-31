use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::mpsc::channel;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use bytesize::ByteSize;
use bzip2::bufread::BzDecoder;
use cpio::NewcReader;
use elf::abi::SHT_NOBITS;
use elf::endian::AnyEndian;
use elf::ElfBytes;
use flate2::read::GzDecoder;
use itertools::Itertools;
use log::{info, warn};
use path_absolutize::*;
use rayon::prelude::*;
use rpm::CompressionType;
use walkdir::WalkDir;
use xz2::read::XzDecoder;

extern crate log;

pub const DEBUG_INFO_PATH: &str = "/usr/lib/debug";
const DWZ_DEBUG_INFO_PATH: &str = "/usr/lib/debug/.dwz/";
const DEBUG_INFO_BUILD_ID_PATH: &str = "/usr/lib/debug/.build-id/";
const BUILD_ID_ELF_PREFIX: [u8; 8] = [0x03, 0x0, 0x0, 0x0, 0x47, 0x4e, 0x55, 0x0];
const BUILD_CHARS: usize = 20;

pub type BuildId = [u8; BUILD_CHARS];

#[derive(Debug)]
enum RPMKind {
    Binary,
    DebugInfo { build_ids: HashMap<BuildId, String> },
    DebugSource,
}

#[derive(Debug)]
struct RPMFile {
    arch: String,
    source_rpm: String,
    name: String,

    path: String,
    kind: RPMKind,
}

#[derive(Debug)]
pub struct DebugInfoRPM {
    pub rpm_path: String,
    pub binary_rpm_path: Option<String>,
    pub source_rpm: Option<String>,

    pub build_id_to_path: HashMap<BuildId, String>,
}

pub struct Server {
    pub root_path: String,
    pub debug_info_rpms: Vec<Arc<DebugInfoRPM>>,

    pub build_ids: HashMap<BuildId, Arc<DebugInfoRPM>>,
    pub total_byte_size: u64,
}

impl Server {
    pub fn new(root_folder: &str) -> Self {
        Server {
            root_path: root_folder.to_string(),
            debug_info_rpms: Vec::new(),
            build_ids: HashMap::new(),
            total_byte_size: 0,
        }
    }

    pub fn walk(&mut self) {
        let mut files = Vec::new();
        for entry in WalkDir::new(self.root_path.clone()) {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file()
                && entry.path().extension().is_some_and(|e| e == "rpm")
            {
                let path = entry.path().to_str();
                match path {
                    Some(path) => {
                        files.push(path.to_string());
                    }
                    None => warn!("invalid RPM file path {entry:?}"),
                }
            }
        }

        self.total_byte_size = files
            .iter()
            .map(|f| std::fs::metadata(f).unwrap().len())
            .sum();
        info!(
            "walking {} RPM files ({})",
            files.len(),
            ByteSize(self.total_byte_size)
        );

        let (rx, tx) = channel();

        files.par_iter().for_each_with(rx, |rx, path| {
            let _ = rx.send(self.analyze_file(path));
        });

        let mut rpms = Vec::new();

        for item in tx.iter() {
            match item {
                Ok(rpm_file) => rpms.push(rpm_file),
                Err(error) => warn!("could not analyze RPM: {error}"),
            }
        }

        // First iterate the source RPM filies and create a map we can later use for construction
        // of the DebugInfoRPM entires.
        let mut source_rpm_map = HashMap::new();
        for rpm in &rpms {
            if let RPMKind::DebugSource = rpm.kind {
                source_rpm_map.insert((&rpm.arch, &rpm.source_rpm), rpm);
            }
        }

        // Second iterate the binary RPM files and also create a map. We need to include canonical
        // package name in the map.
        let mut binary_rpm_map = HashMap::new();
        for rpm in &rpms {
            if let RPMKind::Binary = rpm.kind {
                binary_rpm_map.insert((&rpm.arch, &rpm.source_rpm, &rpm.name), rpm);
            }
        }

        // Now we can construct DebugInfoRPM entries and find the corresponding Binary and DebugSource packages.
        for rpm in &rpms {
            if let RPMKind::DebugInfo { build_ids } = &rpm.kind {
                let debug_info = Arc::new(DebugInfoRPM {
                    rpm_path: rpm.path.clone(),
                    binary_rpm_path: binary_rpm_map
                        .get(&(&rpm.arch, &rpm.source_rpm, &rpm.name))
                        .map(|r| r.path.clone()),
                    source_rpm: source_rpm_map
                        .get(&(&rpm.arch, &rpm.source_rpm))
                        .map(|r| r.path.clone()),
                    build_id_to_path: build_ids.clone(),
                });

                self.debug_info_rpms.push(debug_info.clone());
                // Construct the Server state build-id mapping to DebugInfoRPM entries.
                for build_id in debug_info.build_id_to_path.keys() {
                    self.build_ids.insert(*build_id, debug_info.clone());
                }
            }
        }
    }

    fn analyze_file(&self, rpm_path: &str) -> anyhow::Result<RPMFile> {
        let rpm_file = std::fs::File::open(rpm_path)?;
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        let header = rpm::PackageMetadata::parse(&mut buf_reader)?;

        let name = header.get_name()?;
        let is_debug_info_rpm = name.ends_with("-debuginfo");
        let canonical_name = name.strip_suffix("-debuginfo").unwrap_or(name).to_string();

        let source_rpm = header.get_source_rpm()?.to_string();
        let arch = header.get_arch()?.to_string();
        let rpm_path = rpm_path.to_string();

        let mut build_ids = HashMap::new();

        let mut contains_dwz = false;
        for file_entry in header.get_file_entries()? {
            let path = file_entry.path;
            if is_debug_info_rpm {
                if path.starts_with(DEBUG_INFO_BUILD_ID_PATH)
                    && path.extension().is_some_and(|e| e == "debug")
                {
                    let mut build_id = path
                        .parent()
                        .context("parent must exist")?
                        .file_name()
                        .context("direct name must exist")?
                        .to_str()
                        .context("filename should be valid")?
                        .to_string();
                    build_id.push_str(
                        path.file_stem()
                            .context("file stem expected")?
                            .to_str()
                            .context("valid path expected")?,
                    );
                    let build_id = self.parse_build_id(&build_id);
                    match build_id {
                        Ok(build_id) => {
                            let target = path
                                .parent()
                                .context("filename must have a parent")?
                                .join(file_entry.linkto.clone());
                            build_ids.insert(
                                build_id,
                                target
                                    .as_path()
                                    .absolutize()?
                                    .to_str()
                                    .context("symlink target path must be valid")?
                                    .to_string(),
                            );
                        }
                        Err(_error) => {
                            // warn!("{rpm_path} {path:?} {_error}");
                        }
                    }
                } else if path.starts_with(DWZ_DEBUG_INFO_PATH) {
                    contains_dwz = true;
                }
            }
        }

        // Right now, there is a missing symlink from a build-id to the .dwz files in the RPM container and
        // so we need to parse it in the ELF file.
        if contains_dwz {
            if let Some((build_id, path)) = self.get_build_id_for_dwz(&rpm_path) {
                build_ids.insert(build_id, path);
            }
        }

        let kind = if is_debug_info_rpm {
            RPMKind::DebugInfo { build_ids }
        } else if name.ends_with("-debugsource") {
            RPMKind::DebugSource
        } else {
            RPMKind::Binary
        };
        Ok(RPMFile {
            arch,
            source_rpm,
            name: canonical_name,
            path: rpm_path,
            kind,
        })
    }

    fn get_rpm_file_stream(
        &self,
        path: &str,
        file_selector: impl Fn(&str) -> bool,
    ) -> anyhow::Result<(NewcReader<impl Read>, String)> {
        let rpm_file = std::fs::File::open(path).context("cannot open RPM file")?;

        let mut buf_reader = std::io::BufReader::new(rpm_file);
        let header = rpm::PackageMetadata::parse(&mut buf_reader)?;
        let compressor = header.get_payload_compressor();
        let mut decoder: Box<dyn BufRead> = match compressor? {
            CompressionType::Zstd => Box::new(BufReader::new(
                zstd::stream::Decoder::new(buf_reader).context("ZSTD decoded failed")?,
            )),
            CompressionType::Gzip => Box::new(BufReader::new(GzDecoder::new(buf_reader))),
            CompressionType::Bzip2 => Box::new(BufReader::new(BzDecoder::new(buf_reader))),
            CompressionType::Xz => Box::new(BufReader::new(XzDecoder::new(buf_reader))),
            CompressionType::None => Box::new(buf_reader),
        };

        loop {
            let archive = NewcReader::new(decoder).context("CPIO decoder failed")?;
            let entry = archive.entry();
            if entry.is_trailer() {
                break;
            }
            let mut name = entry.name().to_string();
            if name.starts_with('.') {
                name = String::from_iter(name.chars().skip(1));
            }
            let file_size = entry.file_size() as usize;

            if file_selector(&name) && file_size > 0 {
                return Ok((archive, name.clone()));
            } else {
                decoder = archive.finish().unwrap();
            }
        }

        Err(anyhow!("file not found in the archive"))
    }

    fn get_build_id_for_dwz(&self, file: &str) -> Option<(BuildId, String)> {
        // For now, let's parse '.note.gnu.build-id' section without any ELF library.
        // Luckily, the created .dwz files (e.g. /usr/lib/debug/.dwz/foo.x86_64) have only a limited
        // number of ELF sections and the note is section is at the very beginning.
        //
        // See SHT_NOTE for a more detail specification. Our note contains "GNU\0" followed by the Build-Id.

        if let Ok((mut stream, name)) =
            self.get_rpm_file_stream(file, |name| name.starts_with(DWZ_DEBUG_INFO_PATH))
        {
            let mut data = vec![0; 256];
            let _ = stream.read_exact(&mut data);
            let mut heystack = data.as_slice();
            for _ in 0..(data.len() - BUILD_ID_ELF_PREFIX.len() - BUILD_CHARS) {
                if heystack.starts_with(&BUILD_ID_ELF_PREFIX) {
                    let build_id = heystack
                        .iter()
                        .skip(BUILD_ID_ELF_PREFIX.len())
                        .take(BUILD_CHARS)
                        .copied()
                        .collect_vec();
                    let build_id = BuildId::try_from(build_id);
                    if let Ok(build_id) = build_id {
                        return Some((build_id, name));
                    } else {
                        break;
                    }
                } else {
                    // Shift the heystack by one byte and continue
                    heystack = &heystack[1..];
                }
            }
        }

        None
    }

    pub fn get_binary_rpm_for_build_id(&self, build_id: &BuildId) -> Option<(String, String)> {
        if let Some(debug_info_rpm) = self.build_ids.get(build_id) {
            if let Some(filename) = debug_info_rpm.build_id_to_path.get(build_id) {
                let filename = filename
                    .strip_suffix(".debug")
                    .unwrap()
                    .strip_prefix(DEBUG_INFO_PATH)
                    .unwrap()
                    .to_string();
                if let Some(binary_rpm_path) = &debug_info_rpm.binary_rpm_path {
                    return Some((binary_rpm_path.clone(), filename));
                }
            }
        }

        None
    }

    pub fn read_rpm_file(&self, rpm_file: &str, file: &str) -> Option<Vec<u8>> {
        info!("reading RPM file {rpm_file}");
        if let Ok((mut stream, _)) = self.get_rpm_file_stream(rpm_file, |f| f == file) {
            info!("found RPM file: {file}");
            let mut content = Vec::new();
            let _ = stream.read_to_end(&mut content);
            Some(content)
        } else {
            None
        }
    }

    pub fn read_rpm_file_section(
        &self,
        rpm_file: &str,
        file: &str,
        section: &str,
    ) -> Option<Vec<u8>> {
        if let Some(data) = self.read_rpm_file(rpm_file, file) {
            if let Ok(elf_file) = ElfBytes::<AnyEndian>::minimal_parse(data.as_slice()) {
                if let Ok(section) = elf_file.section_header_by_name(section) {
                    let section = section?;
                    if section.sh_type == SHT_NOBITS {
                        return None;
                    }

                    if let Ok(section_data) = elf_file.section_data(&section) {
                        let mut result = Vec::new();
                        section_data.0.clone_into(&mut result);
                        return Some(result);
                    }
                }
            }
        }
        None
    }

    pub fn parse_build_id(&self, id: &str) -> anyhow::Result<BuildId> {
        let array = hex::decode(id)?;
        if array.len() != BUILD_CHARS {
            Err(anyhow!(
                "Invalid build-id length: {}, expected {BUILD_CHARS}",
                array.len()
            ))
        } else {
            Ok(BuildId::try_from(array.as_slice())?)
        }
    }
}
