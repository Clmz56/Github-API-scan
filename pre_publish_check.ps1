# Pre-Publish Check Script
# 在发布到 GitHub 之前运行此脚本进行最后检查

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  GitHub 发布前安全检查" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$hasError = $false

# 检查 1: 搜索敏感的 token 模式
Write-Host "[1/6] 检查代码中的敏感 tokens..." -ForegroundColor Yellow
$tokenPatterns = @("github_pat_11", "ghp_[a-zA-Z0-9]{36}")
$sensitiveFiles = Get-ChildItem -Path . -Include *.py -Recurse | Select-String -Pattern $tokenPatterns | Where-Object { $_.Line -notmatch "示例|example|xxx|yyy|zzz|comment|#" }
if ($sensitiveFiles) {
    Write-Host "  ⚠️  警告: 发现可能的真实 tokens!" -ForegroundColor Red
    $sensitiveFiles | ForEach-Object { Write-Host "    - $($_.Filename):$($_.LineNumber)" -ForegroundColor Red }
    $hasError = $true
} else {
    Write-Host "  ✅ 未发现可疑的 tokens" -ForegroundColor Green
}

# 检查 2: 确认数据库文件不在 Git 追踪中
Write-Host "[2/6] 检查数据库文件..." -ForegroundColor Yellow
if (Test-Path "leaked_keys.db") {
    $gitStatus = git ls-files --error-unmatch leaked_keys.db 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ⚠️  警告: leaked_keys.db 在 Git 追踪中!" -ForegroundColor Red
        Write-Host "    运行: git rm --cached leaked_keys.db" -ForegroundColor Red
        $hasError = $true
    } else {
        Write-Host "  ✅ 数据库文件已被正确忽略" -ForegroundColor Green
    }
} else {
    Write-Host "  ✅ 数据库文件不存在" -ForegroundColor Green
}

# 检查 3: 确认 config_local.py 不在 Git 追踪中
Write-Host "[3/6] 检查本地配置文件..." -ForegroundColor Yellow
if (Test-Path "config_local.py") {
    $gitStatus = git ls-files --error-unmatch config_local.py 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ⚠️  警告: config_local.py 在 Git 追踪中!" -ForegroundColor Red
        Write-Host "    运行: git rm --cached config_local.py" -ForegroundColor Red
        $hasError = $true
    } else {
        Write-Host "  ✅ 本地配置文件已被正确忽略" -ForegroundColor Green
    }
} else {
    Write-Host "  ✅ 本地配置文件不存在" -ForegroundColor Green
}

# 检查 4: 确认 .gitignore 存在并包含必要条目
Write-Host "[4/6] 检查 .gitignore 文件..." -ForegroundColor Yellow
if (Test-Path ".gitignore") {
    $gitignoreContent = Get-Content .gitignore -Raw
    $requiredEntries = @("*.db", "*.sqlite", ".env", "config_local.py", "__pycache__", "*.log")
    $missing = @()
    foreach ($entry in $requiredEntries) {
        if ($gitignoreContent -notmatch [regex]::Escape($entry)) {
            $missing += $entry
        }
    }
    if ($missing.Count -gt 0) {
        Write-Host "  ⚠️  警告: .gitignore 缺少以下条目:" -ForegroundColor Red
        $missing | ForEach-Object { Write-Host "    - $_" -ForegroundColor Red }
        $hasError = $true
    } else {
        Write-Host "  ✅ .gitignore 包含所有必要条目" -ForegroundColor Green
    }
} else {
    Write-Host "  ⚠️  错误: .gitignore 文件不存在!" -ForegroundColor Red
    $hasError = $true
}

# 检查 5: 确认必要的文档文件存在
Write-Host "[5/6] 检查文档文件..." -ForegroundColor Yellow
$requiredDocs = @("README.md", "LICENSE", "CONTRIBUTING.md", "QUICKSTART.md", ".env.example")
$missingDocs = @()
foreach ($doc in $requiredDocs) {
    if (-not (Test-Path $doc)) {
        $missingDocs += $doc
    }
}
if ($missingDocs.Count -gt 0) {
    Write-Host "  ⚠️  警告: 缺少以下文档文件:" -ForegroundColor Yellow
    $missingDocs | ForEach-Object { Write-Host "    - $_" -ForegroundColor Yellow }
} else {
    Write-Host "  ✅ 所有文档文件都存在" -ForegroundColor Green
}

# 检查 6: 检查 Git 状态
Write-Host "[6/6] 检查 Git 状态..." -ForegroundColor Yellow
try {
    git status --short | Out-Null
    $untracked = git ls-files --others --exclude-standard
    if ($untracked) {
        Write-Host "  ℹ️  发现未追踪的文件:" -ForegroundColor Cyan
        $untracked | ForEach-Object { 
            Write-Host "    - $_" -ForegroundColor Cyan
        }
    } else {
        Write-Host "  ✅ 没有未追踪的文件" -ForegroundColor Green
    }
} catch {
    Write-Host "  ⚠️  警告: Git 仓库未初始化" -ForegroundColor Yellow
}

# 总结
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
if ($hasError) {
    Write-Host "  ❌ 检查失败!" -ForegroundColor Red
    Write-Host "  请修复上述问题后再发布到 GitHub" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Cyan
    exit 1
} else {
    Write-Host "  ✅ 所有检查通过!" -ForegroundColor Green
    Write-Host "  可以安全地发布到 GitHub" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "下一步:" -ForegroundColor Yellow
    Write-Host "  1. git init" -ForegroundColor White
    Write-Host "  2. git add ." -ForegroundColor White
    Write-Host '  3. git commit -m "Initial commit"' -ForegroundColor White
    Write-Host "  4. 在 GitHub 创建仓库" -ForegroundColor White
    Write-Host "  5. git remote add origin <your-repo-url>" -ForegroundColor White
    Write-Host "  6. git push -u origin main" -ForegroundColor White
    Write-Host ""
    Write-Host "详细步骤请参考: GITHUB_PUBLISH_GUIDE.md" -ForegroundColor Cyan
    exit 0
}
