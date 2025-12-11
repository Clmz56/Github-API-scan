# 快速开始指南

本指南帮助你快速配置和运行 GitHub Secret Scanner Pro。

## 📋 前置要求

- Python 3.9 或更高版本
- GitHub Personal Access Token
- （可选）代理服务器（如在中国大陆使用）

## 🚀 5 分钟快速开始

### 1. 克隆项目

```powershell
git clone https://github.com/YOUR_USERNAME/github-secret-scanner.git
cd github-secret-scanner
```

### 2. 安装依赖

```powershell
# 安装依赖包
pip install -r requirements.txt

# 或使用国内镜像加速
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 3. 配置 GitHub Token

**方式一：创建本地配置文件（推荐）**

```powershell
# 复制配置模板
Copy-Item config_local.py.example config_local.py

# 使用记事本编辑配置文件
notepad config_local.py
```

在 `config_local.py` 中填入你的 GitHub Token：

```python
GITHUB_TOKENS = [
    "ghp_你的真实Token",
    # 可以添加多个 token
]
```

**方式二：使用环境变量**

```powershell
# Windows PowerShell
$env:GITHUB_TOKENS = "ghp_你的Token1,ghp_你的Token2"

# 或永久设置
[System.Environment]::SetEnvironmentVariable("GITHUB_TOKENS", "ghp_你的Token", "User")
```

**如何获取 GitHub Token:**

1. 访问 https://github.com/settings/tokens
2. 点击 "Generate new token (classic)"
3. 勾选 `public_repo` 权限
4. 生成并复制 token

### 4. 运行程序

```powershell
# 直接运行
python main.py

# 如果需要代理
python main.py --proxy http://127.0.0.1:7890
```

### 5. 查看结果

程序会在终端显示 Rich TUI 界面，实时展示扫描进度。

结果保存在 SQLite 数据库 `leaked_keys.db` 中。

## 📊 导出结果

```powershell
# 导出为文本文件
python main.py --export output.txt

# 导出为 CSV（包含详细元数据）
python main.py --export-csv results.csv

# 查看统计信息
python main.py --stats
```

## ⚙️ 高级配置

### 配置代理

如果需要使用代理访问 GitHub API：

```python
# 在 config_local.py 中添加
PROXY_URL = "http://127.0.0.1:7890"
```

或使用环境变量：

```powershell
$env:PROXY_URL = "http://127.0.0.1:7890"
```

### 调整线程数

```python
# 在 config_local.py 中调整
CONSUMER_THREADS = 20  # 验证器线程数，可根据机器性能调整
```

### 修改搜索关键词

编辑 `config.py` 中的 `search_keywords` 列表，自定义搜索策略。

## 🐛 故障排除

### 问题：提示未配置 GitHub Tokens

**解决方案：**
- 确认已创建 `config_local.py` 文件
- 或设置了环境变量 `GITHUB_TOKENS`
- Token 格式正确（以 `ghp_` 或 `github_pat_` 开头）

### 问题：GitHub API 速率限制

**解决方案：**
- 添加更多 GitHub Tokens 到配置文件
- 单个 token: 30次/分钟
- 多个 token 轮询：速率成倍增长

### 问题：网络连接失败

**解决方案：**
- 检查代理设置是否正确
- 尝试使用 `--proxy` 参数指定代理
- 确认代理服务正常运行

### 问题：安装依赖失败

**解决方案：**
```powershell
# 使用国内镜像
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 或分步安装
pip install aiohttp[speedups] PyGithub rich loguru
```

## 📚 下一步

- 阅读完整 [README.md](README.md) 了解详细功能
- 查看 [GITHUB_PUBLISH_GUIDE.md](GITHUB_PUBLISH_GUIDE.md) 学习如何发布项目
- 自定义 `config.py` 中的搜索策略和正则表达式

## 💡 提示

1. **安全第一**: 永远不要将包含真实 token 的 `config_local.py` 提交到 Git
2. **多 Token**: 配置多个 GitHub Token 可大幅提升扫描速度
3. **代理设置**: 在中国大陆建议使用代理以提高稳定性
4. **数据库**: `leaked_keys.db` 会自动去重，可以多次运行程序积累数据

---

**遇到问题？** 查看 [Issues](https://github.com/YOUR_USERNAME/github-secret-scanner/issues) 或提交新问题。
