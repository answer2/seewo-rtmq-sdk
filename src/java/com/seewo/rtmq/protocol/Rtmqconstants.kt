package com.seewo.rtmq.model

object RtmqConstants {
    const val DEFAULT_HEARTBEAT = 10    // seconds
    const val TIMEOUT = 30              // seconds
    const val TOPIC_IM = "im/data"
    const val TOPIC_PUSH = "push/data"
    const val VERSION = "1.0.0"
    const val SIGNSURE = "7db047a67a9d7293850ac69d14cc82bf"
    const val MSG_SIZE_LIMIT = 10240    // 10KB
}

enum class CmdCode(val value: Int) {
    CONNECT(1),
    CONNECT_ACK(2),
    DISCONNECT(3),
    DISCONNECT_ACK(4),
    UPDATE(5),
    UPDATE_ACK(6),
    BIND(7),
    BIND_ACK(8),
    UNBIND(9),
    UNBIND_ACK(10),
    PING(11),
    PONG(12),
    PUSH(13),
    PUSH_ACK(14),
    CALL(15),
    CALL_ACK(16);

    companion object {
        fun fromValue(value: Int): CmdCode? = entries.find { it.value == value }
    }
}

enum class PlatformType(val value: Int) {
    UNKNOWN(0),
    WIN(1),
    ANDROID(2),
    IOS(3),
    WEB(4),
    SEEWOOS(5),
    MAC(6),
    LINUX(7)
}

enum class CloseType {
    USER_CLOSE,
    CONNECT_TIMEOUT,
    CONNECT_FAIL,
    RECONNECT_FAIL,
    HEARTBEAT_FAIL,
    WEBSOCKET_ERROR,
    UNKNOWN
}

enum class ConnectType {
    USER_CONNECT,
    RECONNECT
}

data class CloseReason(val code: Int, val message: String)
data class ConnectReason(val code: Int, val message: String)

object CloseReasons {
    fun userClose() = CloseReason(0, "断开连接")
    fun connectTimeout() = CloseReason(-10000, "连接超时")
    fun connectFail(code: Int, message: String) = CloseReason(code, message)
    fun reconnectFail() = CloseReason(-10001, "重新连接失败")
    fun heartbeatFail() = CloseReason(-10002, "连接异常")
    fun websocketError() = CloseReason(-10003, "连接websocket地址出错")
    fun unknown() = CloseReason(-10004, "未知原因")
}

object ConnectReasons {
    fun userConnect() = ConnectReason(0, "成功连接")
    fun reconnect() = ConnectReason(0, "重新连接成功")
}