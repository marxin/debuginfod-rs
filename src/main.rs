use std::collections::HashMap;
use std::io::Read;
use std::sync::mpsc::channel;

use anyhow::Result;
use path_absolutize::*;
use rayon::prelude::*;
use rocket::response::status::NotFound;
use rocket::time::Instant;
use rpm;
use rpm::CompressionType;
use walkdir::WalkDir;

#[macro_use]
extern crate rocket;
use rocket::State;

const ARCH_MAPPING: [&str; 1] = ["x86_64"];
const DEBUG_INFO_PATH_PREFIX: &str = "/usr/lib/debug/.build-id/";
const BUILD_ID_PREFIX: [u8; 8] = [0x03, 0x0, 0x0, 0x0, 0x47, 0x4e, 0x55, 0x0];

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RPMSourceKey {
    arch: usize,
    source_rpm: String,
}

#[derive(Debug)]
enum RPMContent {
    Binary,
    DebugInfo { build_ids: HashMap<String, String> },
    DebugSource,
}

#[derive(Debug)]
struct RPMFile {
    source: RPMSourceKey,
    path: String,
    content: RPMContent,
}

struct Server {
    root_path: String,
    binary_rpms: HashMap<RPMSourceKey, RPMFile>,
    debug_info_rpms: HashMap<RPMSourceKey, RPMFile>,
    debug_source_rpms: HashMap<RPMSourceKey, RPMFile>,
    build_ids: HashMap<String, RPMSourceKey>,
}

impl Server {
    fn new(root_folder: &str) -> Self {
        Server {
            root_path: String::from(root_folder),
            binary_rpms: HashMap::new(),
            debug_info_rpms: HashMap::new(),
            debug_source_rpms: HashMap::new(),
            build_ids: HashMap::new(),
        }
    }

    fn walk(&mut self) {
        let mut files = Vec::new();
        for entry in WalkDir::new(self.root_path.clone()) {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                files.push(String::from(entry.path().to_str().unwrap()));
            }
        }

        let (rx, tx) = channel();

        files.par_iter().for_each_with(rx, |rx, path| {
            let _ = rx.send(self.analyze_file(path));
        });

        for item in tx.iter() {
            if let Ok(rpm_file) = item {
                let map = match rpm_file.content {
                    RPMContent::Binary => &mut self.binary_rpms,
                    RPMContent::DebugInfo { .. } => &mut self.debug_info_rpms,
                    RPMContent::DebugSource => &mut self.debug_source_rpms,
                };
                map.insert(rpm_file.source.clone(), rpm_file);
            } else {
                todo!();
            }
        }

        // save all build-ids for the future look up
        for (source_key, rpm) in &self.debug_info_rpms {
            if let RPMContent::DebugInfo { build_ids } = &rpm.content {
                for (build_id, _) in build_ids {
                    // TODO: remove clonning
                    self.build_ids.insert(build_id.clone(), source_key.clone());
                }
            }
        }
    }

    fn analyze_file(&self, path: &str) -> Result<RPMFile> {
        let rpm_file = std::fs::File::open(path)?;
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        // TODO: use ?
        let header = rpm::PackageMetadata::parse(&mut buf_reader).unwrap();

        let name = header.get_name().unwrap();
        let source_rpm = String::from(header.get_source_rpm().unwrap());
        let arch = header.get_arch().unwrap();
        let source = RPMSourceKey {
            arch: ARCH_MAPPING.iter().position(|&item| item == arch).unwrap(),
            source_rpm,
        };
        let path = String::from(path);

        let is_debug_info_rpm = name.ends_with("-debuginfo");
        let mut build_ids = HashMap::new();

        // TODO: called `Result::unwrap()` on an `Err` value: TagNotFound("RPMTAG_FILEMODES")

        let mut contains_dwz = false;
        if let Ok(entries) = header.get_file_entries() {
            for file_entry in entries {
                let path = file_entry.path;
                if is_debug_info_rpm {
                    if path.starts_with(DEBUG_INFO_PATH_PREFIX)
                        && path.extension().is_some_and(|e| e == ".debug")
                    {
                        let mut build_id = String::from(
                            path.parent()
                                .unwrap()
                                .file_name()
                                .unwrap()
                                .to_str()
                                .unwrap(),
                        );
                        build_id.push_str(path.file_stem().unwrap().to_str().unwrap());

                        let target = path.parent().unwrap().join(file_entry.linkto.clone());
                        build_ids.insert(
                            build_id,
                            String::from(target.as_path().absolutize().unwrap().to_str().unwrap()),
                        );
                    } else if path
                        .parent()
                        .is_some_and(|p| p.file_name().unwrap() == ".dwz")
                    {
                        contains_dwz = true;
                    }
                }
            }
        }

        if contains_dwz {
            if let Some((build_id, path)) = self.get_build_id_for_dwz(&path) {
                build_ids.insert(build_id, path);
            }
        }

        let content = if is_debug_info_rpm {
            RPMContent::DebugInfo { build_ids }
        } else if name.ends_with("-debugsource") {
            RPMContent::DebugSource
        } else {
            RPMContent::Binary
        };
        Ok(RPMFile {
            source,
            path,
            content,
        })
    }

    fn get_build_id_for_dwz(&self, path: &str) -> Option<(String, String)> {
        let rpm_file = std::fs::File::open(path).unwrap();

        let mut buf_reader = std::io::BufReader::new(rpm_file);
        // TODO: use ?
        let header = rpm::PackageMetadata::parse(&mut buf_reader).unwrap();

        let compressor = header.get_payload_compressor();
        if compressor.is_err() || compressor.ok().unwrap() != CompressionType::Zstd {
            // TODO: fix
            return None;
        }

        let mut decoder = zstd::stream::Decoder::new(buf_reader).unwrap();

        loop {
            let mut archive = cpio::NewcReader::new(decoder).unwrap();
            let entry = archive.entry();
            if entry.is_trailer() {
                break;
            }
            let name = String::from(entry.name());
            let file_size = entry.file_size() as usize;

            if name.contains("usr/lib/debug/.dwz/") && file_size > 0 {
                let mut data = vec![0; 256];
                let _ = archive.read_exact(&mut data);
                let mut heystack = &data[..];
                // TODO: proper iteration space
                for _ in 0..128 {
                    if heystack.starts_with(&BUILD_ID_PREFIX) {
                        let build_id: Vec<_> = heystack
                            .iter()
                            .skip(BUILD_ID_PREFIX.len())
                            .take(20)
                            .copied()
                            .collect();
                        return Some((hex::encode(build_id), name));
                    } else {
                        heystack = &heystack[1..];
                    }
                }
                break;
            } else {
                decoder = archive.finish().unwrap();
            }
        }

        panic!()
    }
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/buildid/<build_id>/debuginfo")]
fn debuginfo(build_id: String, state: &State<Server>) -> Result<String, NotFound<&str>> {
    if let Some(path) = state.build_ids.get(&build_id) {
        Ok(state.debug_info_rpms[path].path.clone())
    } else {
        Err(NotFound("The provided build-id is not found."))
    }
}

#[launch]
fn rocket() -> _ {
    let start = Instant::now();
    let mut server = Server::new("/home/marxin/Data");
    server.walk();
    println!(
        "Parsing took: {} s",
        (Instant::now() - start).as_seconds_f32()
    );

    rocket::build()
        .manage(server)
        .mount("/", routes![index, debuginfo])
}
