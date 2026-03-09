use anyhow::{Context, anyhow};
use chrono::Duration;
use std::env::var;
use std::path::PathBuf;
use tbackup::*;
fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    //循环重复备份并检查
    loop {
        //通过深层括号在休眠前释放句柄
        {
            //记录需要找到的信息
            let source_path = var("SOURCE_PATH").context(fl!("Missing SOURCE_PATH variable."))?;
            let source_path = PathBuf::from(source_path);
            let backup_path = var("BACKUP_PATH").context(fl!("Missing BACKUP_PATH variable."))?;
            let backup_path = PathBuf::from(backup_path);
            let now = chrono::Local::now();
            let s_now = now.format("%Y-%m-%d_%H-%M-%S").to_string();
            let file_name_prefix =
                var("FILENAME_PREFIX").context(fl!("Missing FILENAME_PREFIX variable."))?;
            let file_name = format!("{}_{}.tar.gz", file_name_prefix, s_now);

            //找到最新的备份文件
            let old_newest = find_newest_backup_file(backup_path.as_path())?;
            //计算其哈希值
            let old_hash = compute_file_hash(old_newest.path())?;
            //备份一次
            backup_once(
                source_path.as_path(),
                backup_path.as_path(),
                file_name.clone(),
            )?;
            //计算新备份哈希值
            let new_newest = find_newest_backup_file(backup_path.as_path())?;
            let new_hash = compute_file_hash(new_newest.path())?;
            //检查超过七天的文件
            let Some(v) =
                find_older_than(backup_path, chrono::Utc::now(), Duration::days(7).to_std()?)?
            else {
                return Err(anyhow!(fl!("cannot find files older than 7 days")));
            };
            //收集所有需要删除的文件
            let mut files_need_delete = v;
            if new_hash == old_hash {
                files_need_delete.push(old_newest)
            }
            //去重
            remove_duplicate(&mut files_need_delete);
            //删除
            delete_backup_files(files_need_delete)?;
        }
        //计时休眠
        std::thread::sleep(std::time::Duration::from_hours(1));
    }
}
