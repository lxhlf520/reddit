"""Reddit 纯协议采集系统。

采集范围：帖子列表(t3)、帖子详情+评论树(t1)、用户资料(t2)。
核心策略：curl_cffi Chrome TLS 指纹 + 会话 cookie，绕过 Reddit 边缘的
403 网络安全拦截与 reCAPTCHA 挑战（实测裸客户端直接 403）。
"""

__version__ = "0.1.0"
