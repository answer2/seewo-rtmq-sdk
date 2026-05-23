package com.seewo.rtmq.protocol


import com.google.gson.Gson
import com.seewo.rtmq.model.*
import java.util.Base64

/**
 * Mirrors the JS functions:
 *  - json2RtmqBuf(cmd, seq, dataObj) → ByteArray
 *  - rtmqBuf2Json(binaryBuffer)      → RtmqData
 */
object RtmqSerializer {

    val gson = Gson()

    // ─── json2RtmqBuf ─────────────────────────────────────────────────────

    fun buildPacket(cmd: CmdCode, seq: Long, payload: Any? = null): ByteArray {
        val innerBytes: ByteArray? = when (cmd) {
            CmdCode.CONNECT    -> (payload as? ConnectMsg)?.let { ProtobufCodec.encodeConnect(it) }
            CmdCode.DISCONNECT -> (payload as? DisconnectMsg)?.let { ProtobufCodec.encodeDisconnect(it) }
            CmdCode.BIND       -> (payload as? BindMsg)?.let { ProtobufCodec.encodeBind(it) }
            CmdCode.UNBIND     -> (payload as? UnbindMsg)?.let { ProtobufCodec.encodeUnbind(it) }
            CmdCode.PUSH       -> (payload as? PushMsg)?.let { ProtobufCodec.encodePush(it) }
            CmdCode.PUSH_ACK   -> (payload as? PushAckMsg)?.let { ProtobufCodec.encodePushAck(it) }
            CmdCode.CALL       -> (payload as? CallMsg)?.let { ProtobufCodec.encodeCall(it) }
            CmdCode.PING       -> null  // ping has no body
            else -> null
        }
        return ProtobufCodec.encodeRtmqPack(cmd.value, seq, innerBytes)
    }

    // ─── rtmqBuf2Json ─────────────────────────────────────────────────────

    fun parsePacket(bytes: ByteArray): RtmqData {
        val pack = ProtobufCodec.decodeRtmqPack(bytes)
        val cmd  = CmdCode.fromValue(pack.cmd) ?: return RtmqData(cmd = pack.cmd, seq = pack.seq)

        return when (cmd) {
            CmdCode.CONNECT_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodeConnectAck(it) } ?: ConnectAckMsg()
                RtmqData(
                    cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, cid = ack.cid, uid = ack.uid, token = ack.token,
                    code = ack.code, message = ack.message,
                    data = ack.data
                )
            }
            CmdCode.DISCONNECT_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodeDisconnectAck(it) } ?: DisconnectAckMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, code = ack.code, message = ack.message)
            }
            CmdCode.BIND_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodeBindAck(it) } ?: BindAckMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, cid = ack.cid, uid = ack.uid,
                    code = ack.code, message = ack.message)
            }
            CmdCode.UNBIND_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodeUnbindAck(it) } ?: UnbindAckMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, cid = ack.cid, uid = ack.uid,
                    code = ack.code, message = ack.message)
            }
            CmdCode.PUSH -> {
                val push = pack.data?.let { ProtobufCodec.decodePush(it) } ?: PushMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = push.traceid, topic = push.topic,
                    ack = push.ack, data = push.data, extend = push.extend)
            }
            CmdCode.PUSH_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodePushAck(it) } ?: PushAckMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, topic = ack.topic,
                    code = ack.code, message = ack.message, extend = ack.extend)
            }
            CmdCode.CALL_ACK -> {
                val ack = pack.data?.let { ProtobufCodec.decodeCallAck(it) } ?: CallAckMsg()
                RtmqData(cmd = pack.cmd, seq = pack.seq,
                    traceid = ack.traceid, topic = ack.topic,
                    code = ack.code, message = ack.message,
                    data = ack.data, extend = ack.extend)
            }
            CmdCode.PONG -> RtmqData(cmd = pack.cmd, seq = pack.seq)
            else -> RtmqData(cmd = pack.cmd, seq = pack.seq, data = pack.data)
        }
    }

    // ─── Base64 JSON helpers ───────────────────────────────────────────────

    fun jsonToBase64(obj: Any): String {
        val json = gson.toJson(obj)
        return Base64.getEncoder().encodeToString(json.toByteArray(Charsets.UTF_8))
    }

    fun base64ToJson(encoded: String): String? = try {
        String(Base64.getDecoder().decode(encoded), Charsets.UTF_8)
    } catch (e: Exception) { null }

    inline fun <reified T> base64ToObject(encoded: String): T? = try {
        val json = base64ToJson(encoded) ?: return null
        gson.fromJson(json, T::class.java)
    } catch (e: Exception) { null }

    fun base64ToBytes(encoded: String): ByteArray? = try {
        Base64.getDecoder().decode(encoded)
    } catch (e: Exception) { null }

    fun bytesToBase64(bytes: ByteArray): String =
        Base64.getEncoder().encodeToString(bytes)
}