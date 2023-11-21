use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;

use anyhow::Result;
use cpio::NewcReader;
use path_absolutize::*;
use rayon::prelude::*;
use rocket::time::Instant;
use rpm;
use rpm::CompressionType;
use walkdir::WalkDir;

#[macro_use]
extern crate rocket;
use rocket::State;

const DEBUG_INFO_PATH_PREFIX: &str = "/usr/lib/debug/.build-id/";
const BUILD_ID_PREFIX: [u8; 8] = [0x03, 0x0, 0x0, 0x0, 0x47, 0x4e, 0x55, 0x0];

#[derive(Debug)]
enum RPMKind {
    Binary,
    DebugInfo { build_ids: HashMap<String, String> },
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
struct DebugInfoRPM {
    rpm_path: String,
    binary_rpm_path: Option<String>,
    source_rpm: Option<String>,

    build_id_to_path: HashMap<String, String>,
}

struct Server {
    root_path: String,
    debug_info_rpms: Vec<Arc<DebugInfoRPM>>,

    build_ids: HashMap<String, Arc<DebugInfoRPM>>,
}

impl Server {
    fn new(root_folder: &str) -> Self {
        Server {
            root_path: String::from(root_folder),
            debug_info_rpms: Vec::new(),
            build_ids: HashMap::new(),
        }
    }

    fn walk(&mut self) {
        let mut files = Vec::new();
        for entry in WalkDir::new(self.root_path.clone()) {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file()
                && entry.path().extension().is_some_and(|e| e == "rpm")
            {
                files.push(String::from(entry.path().to_str().unwrap()));
            }
        }

        println!("Walking {} RPM files", files.len());

        let (rx, tx) = channel();

        files.par_iter().for_each_with(rx, |rx, path| {
            let _ = rx.send(self.analyze_file(path));
        });

        let mut rpms = Vec::new();

        for item in tx.iter() {
            if let Ok(rpm_file) = item {
                rpms.push(rpm_file);
            }
        }

        /* First iterate the source RPM filies and create a map we can later use for construction
        of the DebugInfoRPM entires. */
        let mut source_rpm_map = HashMap::new();
        for rpm in &rpms {
            if let RPMKind::DebugSource = rpm.kind {
                source_rpm_map.insert((&rpm.arch, &rpm.source_rpm), rpm);
            }
        }

        /* Second iterate the binary RPM files and also create a map. We need to include canonical
        package name in the map. */
        let mut binary_rpm_map = HashMap::new();
        for rpm in &rpms {
            if let RPMKind::Binary = rpm.kind {
                binary_rpm_map.insert((&rpm.arch, &rpm.source_rpm, &rpm.name), rpm);
            }
        }

        /* Now we can construct DebugInfoRPM entries and find the corresponding Binary and DebugSource packages. */
        for rpm in &rpms {
            if let RPMKind::DebugInfo { build_ids } = &rpm.kind {
                let debug_info = Arc::new(DebugInfoRPM {
                    rpm_path: rpm.path.clone(),
                    binary_rpm_path: binary_rpm_map
                        .get(&(&rpm.arch, &rpm.source_rpm, &rpm.name))
                        .and_then(|r| Some(r.path.clone())),
                    source_rpm: source_rpm_map
                        .get(&(&rpm.arch, &rpm.source_rpm))
                        .and_then(|r| Some(r.path.clone())),
                    build_id_to_path: build_ids.clone(),
                });
                self.debug_info_rpms.push(debug_info);
            }
        }

        /* Construct the Server state build-id mapping to DebugInfoRPM entries. */
        for rpm in &self.debug_info_rpms {
            for build_id in rpm.build_id_to_path.keys() {
                self.build_ids.insert(build_id.clone(), rpm.clone());
            }
        }
    }

    fn analyze_file(&self, path: &str) -> Result<RPMFile> {
        let rpm_file = std::fs::File::open(path)?;
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        // TODO: use ?
        let header = rpm::PackageMetadata::parse(&mut buf_reader).unwrap();

        let name = header.get_name().unwrap();
        let is_debug_info_rpm = name.ends_with("-debuginfo");
        let canonical_name = name.strip_suffix("-debuginfo").unwrap_or(name).to_string();

        let source_rpm = String::from(header.get_source_rpm().unwrap());
        let arch = header.get_arch().unwrap().to_string();
        let path = String::from(path);

        let mut build_ids = HashMap::new();

        let mut contains_dwz = false;
        for file_entry in header.get_file_entries().unwrap() {
            let path = file_entry.path;
            if is_debug_info_rpm {
                if path.starts_with(DEBUG_INFO_PATH_PREFIX)
                    && path.extension().is_some_and(|e| e == "debug")
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

        if contains_dwz {
            if let Some((build_id, path)) = self.get_build_id_for_dwz(&path) {
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
            path,
            kind,
        })
    }

    fn get_rpm_file_stream(
        &self,
        path: &str,
        file_selector: impl Fn(&String) -> bool,
    ) -> Option<(NewcReader<impl Read>, String)> {
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
            let archive = NewcReader::new(decoder).unwrap();
            let entry = archive.entry();
            if entry.is_trailer() {
                break;
            }
            let mut name = String::from(entry.name());
            if name.starts_with('.') {
                name = String::from_iter(name.chars().skip(1));
            }
            let file_size = entry.file_size() as usize;

            // TODO
            if file_selector(&name) && file_size > 0 {
                return Some((archive, name.clone()));
            } else {
                decoder = archive.finish().unwrap();
            }
        }

        None
    }

    fn get_build_id_for_dwz(&self, file: &str) -> Option<(String, String)> {
        if let Some((mut stream, name)) =
            self.get_rpm_file_stream(file, |name| name.contains("usr/lib/debug/.dwz/"))
        {
            let mut data = vec![0; 256];
            let _ = stream.read_exact(&mut data);
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
        }

        None
    }

    fn read_rpm_file(&self, rpm_file: &String, file: &String) -> Option<Vec<u8>> {
        println!("  reading RPM file {rpm_file}");
        if let Some((mut stream, _)) = self.get_rpm_file_stream(rpm_file, |f| f == file) {
            let mut content = Vec::new();
            let _ = stream.read_to_end(&mut content);
            return Some(content);
        } else {
            None
        }
    }
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/buildid/<build_id>/debuginfo")]
fn debuginfo(build_id: String, state: &State<Server>) -> Option<Vec<u8>> {
    if let Some(debug_info_rpm) = state.build_ids.get(&build_id) {
        return state.read_rpm_file(
            &debug_info_rpm.rpm_path,
            &debug_info_rpm.build_id_to_path[&build_id],
        );
    }

    None
}

#[get("/buildid/<build_id>/source/<source_path..>")]
fn source(build_id: String, source_path: PathBuf, state: &State<Server>) -> Option<Vec<u8>> {
    if let Some(debug_info_rpm) = state.build_ids.get(&build_id) {
        if let Some(source_rpm_path) = &debug_info_rpm.source_rpm {
            let mut filename = source_path.to_str().unwrap().to_string();
            // TODO: fix me
            filename.insert(0, '/');
            return state.read_rpm_file(&source_rpm_path, &filename);
        }
    }

    None
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
    println!("Registered {} build-ids", server.build_ids.len());
    println!("For {} DebugInfoRPM entries", server.debug_info_rpms.len());

    for debug_info_rpm in server.debug_info_rpms.iter().take(10) {
        println!("{debug_info_rpm:?}");
    }

    rocket::build()
        .manage(server)
        .mount("/", routes![index, debuginfo, source])
}
