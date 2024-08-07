use std::env::{self};
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use env_logger::Env;
use itertools::Itertools;
use log::info;
extern crate log;
use bytesize::ByteSize;

#[macro_use]
extern crate rocket;
use rocket::State;

use debuginfod_rs::*;

#[get("/")]
fn index() -> &'static str {
    "Welcome to debuginfod-rs server!"
}

#[get("/buildid/<build_id>/debuginfo")]
fn debuginfo(build_id: String, state: &State<Server>) -> Option<Vec<u8>> {
    if let Ok(build_id) = state.parse_build_id(&build_id) {
        if let Some(debug_info_rpm) = state.build_ids.get(&build_id) {
            return state.read_rpm_file(
                &debug_info_rpm.rpm_path,
                &debug_info_rpm.build_id_to_path[&build_id],
            );
        }
    }

    None
}

#[get("/buildid/<build_id>/executable")]
fn executable(build_id: String, state: &State<Server>) -> Option<Vec<u8>> {
    if let Ok(build_id) = state.parse_build_id(&build_id) {
        if let Some((binary_rpm_file, filename)) = state.get_binary_rpm_for_build_id(&build_id) {
            return state.read_rpm_file(&binary_rpm_file, &filename);
        }
    }

    None
}

#[get("/buildid/<build_id>/section/<section_name>")]
fn section(build_id: String, section_name: String, state: &State<Server>) -> Option<Vec<u8>> {
    if let Ok(build_id) = state.parse_build_id(&build_id) {
        if let Some(debug_info_rpm) = state.build_ids.get(&build_id) {
            // First try to find the section in the debug info ELF binary.
            if let Some(data) = state.read_rpm_file_section(
                &debug_info_rpm.rpm_path,
                &debug_info_rpm.build_id_to_path[&build_id],
                &section_name,
            ) {
                return Some(data);
            } else if let Some((binary_rpm_file, filename)) =
                state.get_binary_rpm_for_build_id(&build_id)
            {
                return state.read_rpm_file_section(&binary_rpm_file, &filename, &section_name);
            }
        }
    }

    None
}

#[get("/buildid/<build_id>/source/<source_path..>")]
fn source(build_id: String, source_path: PathBuf, state: &State<Server>) -> Option<Vec<u8>> {
    if let Ok(build_id) = state.parse_build_id(&build_id) {
        if let Some(debug_info_rpm) = state.build_ids.get(&build_id) {
            if let Some(source_rpm_path) = &debug_info_rpm.source_rpm {
                let mut filename = source_path.to_str().unwrap().to_string();
                // Prefix all paths with slash.
                filename.insert(0, '/');
                return state.read_rpm_file(source_rpm_path, &filename);
            }
        }
    }

    None
}

#[launch]
fn rocket() -> _ {
    let arguments = env::args().collect_vec();
    if arguments.len() != 2 {
        println!("Usage: debuginfod-rs folder");
        process::exit(1);
    }

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .init();

    let start = Instant::now();
    let mut server = Server::new(arguments.get(1).unwrap());
    server.walk();

    // trim heap allocation after we parse all the RPM files
    unsafe {
        libc::malloc_trim(0);
    }

    let duration = (Instant::now() - start).as_secs_f32();
    let bytes_per_s = server.total_byte_size as f32 / duration;
    info!(
        "parsing took: {:.2} s ({}/s)",
        duration,
        ByteSize(bytes_per_s.round() as u64)
    );
    info!("registered {} build-ids", server.build_ids.len());
    info!("DebugInfo RPM entries: {}", server.debug_info_rpms.len());

    rocket::build()
        .manage(server)
        .mount("/", routes![index, debuginfo, executable, source, section])
}
