use anyhow::{Context, anyhow};
use chrono::Utc;
use dotenv::var;
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::Digest;
use std::collections::HashSet;
use std::fs::File;
use std::time::SystemTime;
use std::{
    fs,
    io::{BufReader, Read},
    path::Path,
    time::Duration,
};
use tar::Builder;
use walkdir::{DirEntry, WalkDir};

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("目标不是文件夹:{0}")]
    NotFolder(String),
}
/// 格式：无前后`/`或`./`
/// 即`tmp`是正确的`./tmp/`是错误的
pub fn backup_once<S, D, N>(src: S, dst: D, file_name: N) -> anyhow::Result<()>
where
    S: AsRef<Path>,
    D: AsRef<Path>,
    N: AsRef<str>,
{
    let src = src.as_ref();
    let dst = dst.as_ref();
    let file_name = file_name.as_ref();
    let file_path = dst.join(file_name);
    create_tar_gz(src, &file_path)
}
///给定一个日期和时间长度，找到所有在(日期 - 时间长度)之前的文件的信息
pub fn find_older_than<P: AsRef<Path>>(
    bak_path: P,
    time: chrono::DateTime<Utc>,
    dur: Duration,
) -> anyhow::Result<Option<Vec<DirEntry>>> {
    if !bak_path.as_ref().is_dir() {
        return Err(anyhow!("{}{}", fl!(), bak_path.as_ref().display()));
    }
    let walk_dir = WalkDir::new(bak_path);
    let mut ans = Vec::new();
    let date = time - dur;
    for r in walk_dir {
        let dir_entry = r?;
        //必是文件：跳过非文件
        if !dir_entry.metadata()?.is_file() {
            continue;
        }
        //必是新文件：跳过大于指定日期的值
        if dir_entry.metadata()?.modified()? > SystemTime::from(date) {
            continue;
        }
        ans.push(dir_entry);
    }
    if ans.is_empty() {
        return Ok(None);
    }
    anyhow::Ok(Some(ans))
}
///计算单个文件的哈希值，如果不是文件则会报错
pub fn compute_file_hash<P: AsRef<Path>>(file_path: P) -> anyhow::Result<String> {
    let f = fs::OpenOptions::new().read(true).open(file_path)?;
    let mut reader = BufReader::new(f);
    let mut buf = vec![0u8; 4096];
    let mut hasher = sha2::Sha256::new();
    loop {
        let read_num = reader.read(&mut buf)?;
        //读完
        if read_num == 0 {
            break;
        }
        hasher.update(&buf[..read_num]);
    }
    let h = hasher.finalize();
    anyhow::Ok(hex::encode(h))
}
pub fn find_newest_backup_file<P: AsRef<Path>>(bak_path: P) -> anyhow::Result<Option<DirEntry>> {
    let bak_path = bak_path.as_ref();

    // 1. 验证 bak 目录是否存在（提前失败，避免后续无意义遍历）
    if !bak_path.exists() {
        return Err(anyhow::anyhow!(
            "Backup directory does not exist: {:?}",
            bak_path
        ));
    }
    if !bak_path.is_dir() {
        return Err(anyhow::anyhow!("Path is not a directory: {:?}", bak_path));
    }

    // 2. 遍历 bak 目录，过滤出所有文件（排除目录/符号链接），并记录最新文件
    let mut newest_entry: Option<DirEntry> = None;
    let mut newest_mtime = SystemTime::UNIX_EPOCH; // 初始化为 Unix 纪元（最早时间）

    // 遍历目录（非递归，仅当前目录；如需递归可移除 .min_depth(1).max_depth(1)）
    for entry in WalkDir::new(bak_path)
        .min_depth(1) // 排除目录自身
        .max_depth(1) // 仅遍历当前目录（不递归子目录）
        .into_iter()
    {
        // 处理遍历过程中的错误（如权限不足、文件被删除）
        let entry = entry.with_context(|| format!("Failed to traverse entry in {:?}", bak_path))?;

        // 过滤：仅保留文件（排除目录、符号链接等）
        let metadata = entry
            .metadata()
            .with_context(|| format!("Failed to get metadata for file: {:?}", entry.path()))?;
        if !metadata.is_file() {
            continue;
        }

        // 3. 获取文件的修改时间，对比并更新最新文件
        let mtime = metadata
            .modified()
            .with_context(|| format!("Failed to get modified time for file: {:?}", entry.path()))?;

        // 如果当前文件修改时间更新，则替换最新记录
        if mtime > newest_mtime {
            newest_mtime = mtime;
            newest_entry = Some(entry);
        }
    }

    Ok(newest_entry)
}
///只删除后缀为.gz的文件
pub fn delete_backup_files(mut v: Vec<DirEntry>) -> anyhow::Result<()> {
    //只保留可用的文件
    v.retain(|d| match d.metadata() {
        Ok(m) => m.is_file(),
        Err(_) => false,
    });
    //只保留后缀为.gz的文件
    v.retain(|d| d.path().extension().and_then(|ext| ext.to_str()) == Some("gz"));
    // v.retain(|p| p.path().extension().is_some_and(|ext| ext == "gz"));
    //删除所有文件
    for d in v {
        fs::remove_file(d.path())?;
    }
    anyhow::Ok(())
}
/// 移除 Vec<DirEntry> 中的重复文件条目
/// 1. 先过滤掉所有非文件类型的条目（目录、符号链接等）
/// 2. 基于文件的**规范化路径**去重（解决软链接/相对路径导致的重复）
pub fn remove_duplicate(v: &mut Vec<DirEntry>) {
    // 第一步：过滤非文件条目，同时处理 metadata 获取失败的情况
    v.retain(|e| {
        // 安全获取文件元数据，失败则视为无效条目并移除
        match e.metadata() {
            Ok(meta) => meta.is_file(), // 只保留文件
            Err(_) => false,            // 元数据获取失败的条目直接移除
        }
    });

    // 第二步：去重 - 用 HashSet 记录已见过的文件路径（规范化路径）
    let mut seen_paths = HashSet::new();
    v.retain(|e| {
        // 获取文件的规范化路径（解决软链接、相对路径/绝对路径等导致的重复）
        let canonical_path = match e.path().canonicalize() {
            // 成功获取规范化路径（推荐）
            Ok(path) => path,
            // 失败则退而求其次使用原始路径（如特殊文件/权限不足的情况）
            Err(_) => e.path().to_path_buf(),
        };

        // insert 返回 true = 路径未见过（保留），false = 路径已存在（移除）
        seen_paths.insert(canonical_path)
    });
}

#[macro_export]
macro_rules! fl {
    () => {
        format!("{}:{}|", file!(), line!())
    };
    ($($arg:tt)*) => {
        format!("{}{}", format!("{}:{}|", file!(), line!()), format!($($arg)*))
    };
}

/// 将指定目录/文件打包并压缩为 .tar.gz
/// # 参数
/// - `source_path`: 要打包的文件/目录路径
/// - `tar_gz_path`: 生成的 .tar.gz 文件路径
pub fn create_tar_gz<S, T>(source_path: S, tar_gz_path: T) -> anyhow::Result<()>
where
    S: AsRef<Path>,
    T: AsRef<Path>,
{
    let source_path = source_path.as_ref();
    let tar_gz_path = tar_gz_path.as_ref();
    // 1. 创建输出的 .tar.gz 文件
    let file = File::create_new(tar_gz_path)?;
    // println!("创建输出文件: {:?}", tar_gz_path);

    // 2. 创建 gzip 编码器（指定压缩级别，1=最快，9=最优）
    let gz_encoder = GzEncoder::new(file, Compression::best());

    // 3. 创建 tar 归档构建器，包装编码器
    let mut tar_builder = Builder::new(gz_encoder);

    // 4. 将文件/目录添加到 tar 归档中
    let source = Path::new(source_path);
    if source.is_dir() {
        // 添加整个目录（递归包含所有子文件/目录）
        tar_builder.append_dir_all(source.file_name().context("未能添加整个目录")?, source)?;
        // println!("添加目录: {:?}", source_path);
    } else if source.is_file() {
        // 添加单个文件
        tar_builder.append_file(
            source.file_name().context("未能添加单个文件")?,
            &mut File::open(source)?,
        )?;
        // println!("添加文件: {:?}", source_path);
    } else {
        return Err(anyhow!("路径不可用: {}", source_path.display()));
    }

    // 5. 完成压缩并刷新所有数据到文件
    tar_builder.finish()?;
    // println!("打包完成！生成文件: {:?}", tar_gz_path);

    Ok(())
}
#[allow(clippy::unwrap_used)]
pub fn backup_newest_in<V: AsRef<[DirEntry]>>(v: V) -> anyhow::Result<()> {
    let v = v.as_ref();
    if v.is_empty() {
        return Err(anyhow::anyhow!(fl!("nothing to backup")));
    }
    let mut vm = Vec::with_capacity(v.len());
    for d in v {
        vm.push((d, d.metadata()?.modified()?));
    }
    vm.sort_by_key(|(_, time)| *time);
    // safe unwrap: v is not empty
    let latest = vm.last().unwrap();
    let latest_path = latest.0.path();
    let file_name = latest_path.file_name().context("cannot read file name")?;
    let longterm_backup_path = Path::new(&var("LONGTERM_BACKUP_PATH")?).join(file_name);
    fs::copy(latest_path, &longterm_backup_path).context(fl!(
        "无法复制超时最新文件:{}|{}",
        latest_path.display(),
        longterm_backup_path.display()
    ))?;
    Ok(())
}
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;
    use walkdir::WalkDir;
    #[test]
    fn t() {
        //
    }

    #[test]
    fn test_compute_file_hash() {
        let file_path = "tmp/musl-1.2.5.tar.gz";
        let h = compute_file_hash(file_path).unwrap();
        let ans = "a9a118bbe84d8764da0ea0d28b3ab3fae8477fc7e4085d90102b8596fc7c75e4";
        assert_eq!(h, ans)
    }
    #[test]
    fn test_find_older_than() {
        let now = Utc::now();
        let dur = Duration::from_hours(24 * 7);
        let bak_path = "tmp/test_older_than";
        let v = find_older_than(bak_path, now, dur).unwrap().unwrap();
        let vp = v.iter().map(|d| d.path()).collect::<Vec<_>>();
        let p = r"tmp/test_older_than/musl-1.2.5.tar.gz";
        let ans = vec![Path::new(p)];
        assert_eq!(vp, ans);
    }
    #[test]
    fn test_remove_duplicate() {
        // 遍历当前目录下的文件（模拟测试数据）
        let mut entries: Vec<DirEntry> = WalkDir::new("tmp/test_remove_duplicate/musl-1.2.5")
            .into_iter()
            .filter_map(|e| e.ok())
            .collect();
        //模拟重复
        entries.extend(entries[35..1275].to_vec());

        // 去重前的数量
        let before = entries.len();
        // 执行去重
        remove_duplicate(&mut entries);
        // 去重后的数量
        let after = entries.len();

        println!("去重前：{} 个条目，去重后：{} 个条目", before, after);
        // 验证所有剩余条目都是文件
        assert!(entries.iter().all(|e| e.metadata().unwrap().is_file()));
    }
    #[test]
    fn test_delete_backup_files() -> anyhow::Result<()> {
        // 创建临时 bak 目录
        let temp_dir = tempfile::tempdir()?;
        let bak_path = temp_dir.path().join("bak");
        fs::create_dir(&bak_path)?;

        // 创建测试文件（不同后缀）
        let file1 = bak_path.join("backup1.bak");
        fs::write(&file1, "old backup")?;

        let file2 = bak_path.join("backup2.gz");
        fs::write(&file2, "new backup")?;

        let entries: Vec<DirEntry> = WalkDir::new(bak_path.clone())
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .collect();
        let before = entries.len();
        dbg!(&before);
        delete_backup_files(entries)?;
        let entries: Vec<DirEntry> = WalkDir::new(bak_path)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .collect();
        let after = entries.len();
        dbg!(&after);
        assert_eq!(after, 1);
        Ok(())
    }
    #[test]
    fn test_find_newest_backup_file() -> anyhow::Result<()> {
        // 创建临时 bak 目录
        let temp_dir = tempfile::tempdir()?;
        let bak_path = temp_dir.path().join("bak");
        fs::create_dir(&bak_path)?;

        // 创建测试文件（不同修改时间）
        let file1 = bak_path.join("backup1.bak");
        fs::write(&file1, "old backup")?;
        std::thread::sleep(Duration::from_millis(10)); // 确保时间差

        let file2 = bak_path.join("backup2.bak");
        fs::write(&file2, "new backup")?;

        // 调用函数并验证结果
        let newest_file = find_newest_backup_file(&bak_path)?;
        assert_eq!(newest_file.unwrap().path(), file2);

        Ok(())
    }

    #[test]
    fn test_create_tar_gz() -> anyhow::Result<()> {
        //创建临时文件夹
        let temp_dir = tempfile::tempdir()?;
        let bak_path = temp_dir.path().join("bak");
        fs::create_dir(&bak_path)?;
        //创建一些文件
        let file1 = bak_path.join("backup1.bak");
        fs::write(&file1, "old backup")?;
        let file2 = bak_path.join("backup2.bak");
        fs::write(&file2, "new backup")?;
        let t1 = temp_dir.path().join("t1");
        fs::create_dir(&t1)?;
        // let file3 = t1.join("backup3.bak");
        // fs::write(&file3, "new backup")?;

        let tmp_dst_dir = tempfile::tempdir()?;
        let dst_path = tmp_dst_dir.path();
        let file_name = "haha.tar.gz";
        let file_path = dst_path.join(file_name);
        create_tar_gz(temp_dir, file_path.as_path())?;
        assert!(file_path.exists());
        // fs::copy(file_path, "tmp/haha.tar.gz")?;
        anyhow::Ok(())
    }
    #[test]
    fn test_backup_once() -> anyhow::Result<()> {
        let back_path = r"items4tests\test_backup_once\musl-1.2.5";
        let dst_path = "tmp/test_backup_once";
        let file_name = "haha.tar.gz";
        backup_once(back_path, dst_path, file_name)
    }
}
