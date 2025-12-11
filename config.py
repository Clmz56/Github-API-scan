"""
配置模块 - 集中管理所有配置项

本模块提供：
- 代理配置（必需，中国大陆环境）
- GitHub Token 池（多 Token 轮询）
- 正则表达式库
- 平台默认 URL
"""

import os
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, FrozenSet


# ============================================================================
#                          熍断器配置 (Circuit Breaker)
# ============================================================================

# 受保护域名白名单 - 永远不会被熍断
PROTECTED_DOMAINS: FrozenSet[str] = frozenset({
    # 官方 API
    "api.openai.com",
    "api.anthropic.com",
    "generativelanguage.googleapis.com",
    # Azure 域名后缀
    "openai.azure.com",
    # GitHub 文件下载
    "github.com",
    "raw.githubusercontent.com",
})

# 应用层错误 HTTP 状态码 - 不触发熍断（说明服务器连通性正常）
SAFE_HTTP_STATUS_CODES: FrozenSet[int] = frozenset({
    400,  # Bad Request - 请求格式错误
    401,  # Unauthorized - Key 无效
    403,  # Forbidden - 权限不足
    404,  # Not Found - 端点不存在
    422,  # Unprocessable Entity - 请求参数错误
    429,  # Rate Limit - 被限流
})

# 网关错误 HTTP 状态码 - 触发熍断（说明服务不可用）
CIRCUIT_BREAKER_HTTP_CODES: FrozenSet[int] = frozenset({
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
})

# 熍断器参数
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5   # 连续失败次数阈值
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 60   # 熍断恢复时间（秒）
CIRCUIT_BREAKER_HALF_OPEN_REQUESTS = 3  # 半开状态允许的试探请求数


# ============================================================================
#                              正则表达式库
# ============================================================================

REGEX_PATTERNS = {
    # OpenAI: 标准 key (sk-xxx) 和 project key (sk-proj-xxx)
    # 使用负面断言排除常见假 Key 前缀和后缀
    # 新增: dev|staging|sandbox 等开发/测试环境关键词
    "openai": r'(?<!example_)(?<!test_)(?<!demo_)(?<!fake_)(?<!sample_)(?<!dev_)(?<!staging_)sk-(?:proj-)?(?!(?:placeholder|example|test|demo|your|xxx|fake|sample|dev|staging|sandbox|xxxxxx|abcdef|123456|insert|replace))[a-zA-Z0-9\-_]{20,}',
    
    # Google Gemini: AIza 开头，必须 39 字符
    # 排除明显的测试 Key
    "gemini": r'(?<!test)(?<!example)(?<!sample)(?<!dev)AIza[0-9A-Za-z\-_]{35}',
    
    # Anthropic Claude: sk-ant- 开头
    # 使用负面断言排除假 Key
    # 新增: dev|staging|sandbox 等开发/测试环境关键词
    "anthropic": r'(?<!example_)(?<!test_)(?<!dev_)(?<!staging_)sk-ant-(?!(?:api0|xxx|test|demo|example|sample|dev|staging|sandbox|placeholder))[a-zA-Z0-9\-_]{20,}',
    
    # Azure OpenAI: 32位十六进制（严格匹配）
    # 排除常见假值如全0、全f、全a、全e等
    "azure": r'(?<![a-f0-9])(?!0{32})(?!f{32})(?!a{32})(?!e{32})[a-f0-9]{32}(?![a-f0-9])',
}

# Azure 特征识别正则
AZURE_URL_PATTERN = r'https://[\w\-]+\.openai\.azure\.com'
AZURE_CONTEXT_KEYWORDS = ['azure', 'openai.azure.com', 'azure_endpoint', 'AZURE_OPENAI']

# Base URL 提取正则（用于上下文感知）
BASE_URL_PATTERNS = [
    # 带变量名的 URL 赋值
    r'(?:base_url|api_base|OPENAI_API_BASE|OPENAI_BASE_URL|host|endpoint|api_endpoint|API_URL|proxy_url|PROXY)\s*[=:]\s*["\']?(https?://[^\s"\'<>]+)["\']?',
    # 通用 HTTP URL
    r'(https?://[a-zA-Z0-9\-_.]+(?::\d+)?(?:/[a-zA-Z0-9\-_./]*)?)',
]

# URL 关键词优先级（用于排序提取到的 URL）
URL_PRIORITY_KEYWORDS = ['base', 'api', 'host', 'endpoint', 'proxy', 'openai', 'relay']


# ============================================================================
#                              配置类
# ============================================================================

@dataclass
class Config:
    """
    全局配置类
    
    重要配置项：
    - proxy_url: 代理地址（中国大陆必需）
    - github_tokens: GitHub Token 列表
    """
    
    # ==================== 代理配置 ====================
    # 直连模式（无代理）
    # 如需代理，可设置环境变量 PROXY_URL 或直接修改此处
    proxy_url: str = field(
        default_factory=lambda: os.getenv("PROXY_URL", "")  # 直连模式
    )
    
    # ==================== GitHub Token 池 ====================
    # 多 Token 轮询可有效规避速率限制
    # 未认证: 10次/分钟, 认证: 30次/分钟
    # 多个 Token 可大幅提升扫描速度
    # 
    # 配置方式：
    # 1. 直接在此列表中添加 token（不推荐，易泄露）
    # 2. 设置环境变量 GITHUB_TOKENS（推荐，用逗号分隔多个token）
    # 3. 创建 config_local.py 覆盖此配置（推荐）
    github_tokens: List[str] = field(default_factory=lambda: (
        # 优先从环境变量读取
        os.getenv("GITHUB_TOKENS", "").split(",") if os.getenv("GITHUB_TOKENS") else [
            # ===== 默认为空，请通过环境变量或 config_local.py 配置 =====
            # 示例格式：
            # "ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            # "ghp_yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        ]
    ))
    
    # Token 轮询索引
    _token_index: int = 0
    
    # ==================== 数据库配置 ====================
    db_path: str = "leaked_keys.db"
    
    # ==================== 线程配置 ====================
    consumer_threads: int = 20  # 验证器线程数（IO 密集型，可开多）
    
    # ==================== 网络配置 ====================
    request_timeout: int = 15  # HTTP 请求超时（秒）
    
    # ==================== 熍断器配置 ====================
    circuit_breaker_enabled: bool = True  # 是否启用熍断器
    
    # ==================== 扫描配置 ====================
    context_window: int = 10  # 上下文窗口（前后各 N 行）
    
    # 搜索关键词 - 高精度狙击模式 (Sniper Dorks)
    # 排除测试文件，专注高价值目标
    # 优化: 增加 NOT staging NOT sandbox 排除干扰
    search_keywords: List[str] = field(default_factory=lambda: [
        # === 1. 狙击 .env 文件 (命中率最高) ===
        'filename:.env OPENAI_API_KEY NOT staging NOT sandbox',
        'filename:.env ANTHROPIC_API_KEY NOT staging',
        'filename:.env GEMINI_API_KEY',
        'filename:.env.local OPENAI_API_KEY',
        'filename:.env.production sk-proj- NOT staging',
        
        # === 2. 狙击特定配置文件 ===
        'filename:config.json sk-proj- NOT example NOT test',
        'filename:secrets.yaml api_key NOT staging NOT sandbox',
        'filename:secrets.json openai NOT example',
        'filename:.env.example sk- NOT test NOT dev',
        
        # === 3. 狙击中转站配置 ===
        'filename:.env BASE_URL openai NOT staging',
        'filename:.env OPENAI_BASE_URL NOT sandbox',
        'filename:config.py ONEAPI',
        'new-api sk- NOT test NOT demo',
        
        # === 4. 排除干扰的精准搜索 ===
        'sk-proj- language:python NOT test NOT example NOT mock NOT staging NOT sandbox',
        'sk-ant-api03 language:python NOT test NOT example NOT staging',
        'AIzaSy language:json NOT example NOT test NOT dev',
        
        # === 5. Anthropic Claude 狙击 ===
        'filename:.env CLAUDE_API_KEY NOT staging',
        'filename:.env anthropic_api_key NOT sandbox',
        '"x-api-key" sk-ant- NOT test NOT example',
        
        # === 6. Azure OpenAI ===
        'filename:.env AZURE_OPENAI_API_KEY NOT staging',
        'openai.azure.com api-key NOT example NOT test NOT staging',
    ])
    
    # ==================== 平台默认 URL ====================
    default_base_urls: Dict[str, str] = field(default_factory=lambda: {
        "openai": "https://api.openai.com",
        "gemini": "https://generativelanguage.googleapis.com/v1beta",
        "anthropic": "https://api.anthropic.com",
        "azure": "",  # Azure 需要从上下文提取
    })
    
    @property
    def proxies(self) -> Optional[Dict[str, str]]:
        """返回 requests 代理格式"""
        if self.proxy_url:
            return {"http": self.proxy_url, "https": self.proxy_url}
        return None
    
    def get_token(self) -> str:
        """获取当前 Token"""
        if not self.github_tokens:
            return ""
        return self.github_tokens[self._token_index % len(self.github_tokens)]
    
    def rotate_token(self) -> str:
        """轮换到下一个 Token"""
        if not self.github_tokens:
            return ""
        self._token_index = (self._token_index + 1) % len(self.github_tokens)
        return self.github_tokens[self._token_index]
    
    def get_random_token(self) -> str:
        """随机获取一个 Token"""
        if not self.github_tokens:
            return ""
        return random.choice(self.github_tokens)


# 全局配置实例
config = Config()

# ============================================================================
#                          本地配置覆盖 (config_local.py)
# ============================================================================
# 尝试导入本地配置文件以覆盖默认设置
# config_local.py 应该包含真实的 tokens 和敏感配置
# 该文件已被 .gitignore 忽略，不会被提交到 Git
try:
    from config_local import *
    
    # 如果 config_local.py 定义了 GITHUB_TOKENS，更新配置
    if 'GITHUB_TOKENS' in dir():
        config.github_tokens = GITHUB_TOKENS
    
    # 如果定义了 PROXY_URL，更新配置
    if 'PROXY_URL' in dir() and PROXY_URL:
        config.proxy_url = PROXY_URL
    
    # 如果定义了其他配置项，也可以在此更新
    if 'DB_PATH' in dir():
        config.db_path = DB_PATH
    if 'CONSUMER_THREADS' in dir():
        config.consumer_threads = CONSUMER_THREADS
    if 'REQUEST_TIMEOUT' in dir():
        config.request_timeout = REQUEST_TIMEOUT
    
    print("✅ 已加载本地配置文件 config_local.py")
except ImportError:
    # config_local.py 不存在，使用默认配置
    if not config.github_tokens or not any(config.github_tokens):
        print("⚠️  警告: 未配置 GitHub Tokens！")
        print("   请创建 config_local.py 文件或设置环境变量 GITHUB_TOKENS")
        print("   参考: config_local.py.example")
except Exception as e:
    print(f"⚠️  加载 config_local.py 时出错: {e}")
