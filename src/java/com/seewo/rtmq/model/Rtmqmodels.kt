package com.seewo.rtmq.model

// ─── Protobuf-style data models ─────────────────────────────────────────────
// These mirror the JS proto messages but use Gson-friendly data classes

data class RtmqPack(
    val cmd: Int = 0,
    val seq: Long = 0L,
    val data: ByteArray? = null,
    val extend: ByteArray? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RtmqPack) return false
        return cmd == other.cmd && seq == other.seq &&
                (data?.contentEquals(other.data ?: byteArrayOf()) ?: (other.data == null)) &&
                (extend?.contentEquals(other.extend ?: byteArrayOf()) ?: (other.extend == null))
    }
    override fun hashCode(): Int {
        var result = cmd
        result = 31 * result + seq.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        result = 31 * result + (extend?.contentHashCode() ?: 0)
        return result
    }
}

data class ConnectMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val pwd: String = "",
    val data: ByteArray? = null,
    val will: ByteArray? = null,
    val signsure: String = RtmqConstants.SIGNSURE,
    val version: String = RtmqConstants.VERSION,
    val platform: Int = PlatformType.ANDROID.value,
    val heartbeat: Int = RtmqConstants.DEFAULT_HEARTBEAT
)

data class ConnectAckMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val token: String = "",
    val data: ByteArray? = null,
    val code: Int = 0,
    val message: String = ""
)

data class DisconnectMsg(
    val traceid: String = "",
    val will: ByteArray? = null
)

data class DisconnectAckMsg(
    val traceid: String = "",
    val code: Int = 0,
    val message: String = ""
)

data class BindMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val pwd: String = "",
    val data: ByteArray? = null,
    val will: ByteArray? = null
)

data class BindAckMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val data: ByteArray? = null,
    val code: Int = 0,
    val message: String = ""
)

data class UnbindMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val will: ByteArray? = null
)

data class UnbindAckMsg(
    val traceid: String = "",
    val appid: String = "",
    val cid: String = "",
    val uid: String = "",
    val code: Int = 0,
    val message: String = ""
)

data class PushMsg(
    val traceid: String = "",
    val topic: String = "",
    val ack: Boolean = false,
    val data: ByteArray? = null,
    val extend: ByteArray? = null
)

data class PushAckMsg(
    val traceid: String = "",
    val topic: String = "",
    val extend: ByteArray? = null,
    val code: Int = 0,
    val message: String = ""
)

data class CallMsg(
    val traceid: String = "",
    val topic: String = "",
    val data: ByteArray? = null,
    val extend: ByteArray? = null
)

data class CallAckMsg(
    val traceid: String = "",
    val topic: String = "",
    val data: ByteArray? = null,
    val extend: ByteArray? = null,
    val code: Int = 0,
    val message: String = ""
)

// ─── Application-level data models ──────────────────────────────────────────

/** Decoded packet after binary → JSON parsing */
data class RtmqData(
    val cmd: Int = 0,
    val seq: Long = 0L,
    val traceid: String = "",
    val topic: String = "",
    val cid: String? = null,
    val token: String? = null,
    val uid: String? = null,
    val code: Int = 0,
    val message: String = "",
    val ack: Boolean = false,
    val data: Any? = null,   // Parsed payload (ApiPack or raw bytes/string)
    val extend: ByteArray? = null
)

/** Application-level JSON payload inside data field */
data class ApiPack(
    val method: String = "",
    val time: Long = 0L,
    val size: Int = 0,
    val data: Any? = null
)

/** Init params from caller */
data class NetBaseConfig(
    val host: String,
    val appid: String,
    var cid: String,
    val uid: String,
    val pwd: String,
    val platform: Int = PlatformType.ANDROID.value,
    val data: String? = null,       // base64-encoded auth data
    val will: String? = null,       // base64-encoded will message
    val heartbeat: Int = RtmqConstants.DEFAULT_HEARTBEAT,
    val isAutoReconnect: Boolean = true
)

/** Callbacks for call/send results */
data class RequestCallbacks(
    val onSuccess: ((RtmqData) -> Unit)? = null,
    val onFail: ((Int, String, RtmqData) -> Unit)? = null,
    val onTimeout: ((RtmqData) -> Unit)? = null
)