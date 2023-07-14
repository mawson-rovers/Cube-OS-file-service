use cubeos_service::*;

service_macro!{
    use failure::Error;
    lib::FileService {
        mutation: Donwload => fn download(&self, source_path: String, target_ip: String, target_port: u16, target_path: String) -> Result<()>;
        mutation: Upload => fn upload(&self, source_ip: String, source_port: u16, source_path: String, target_path: String) -> Result<()>;
        mutation: CleanUp => fn cleanup(&self, target_ip: String, target_port: u16, hash: Option<String>) -> Result<()>;
    }
}