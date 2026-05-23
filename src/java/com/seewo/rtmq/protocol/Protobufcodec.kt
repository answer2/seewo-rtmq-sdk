package com.seewo.rtmq.protocol

import com.seewo.rtmq.model.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

/**
 * Minimal protobuf encoder/decoder that mirrors the JS jspb implementation.
 *
 * Wire types:
 *  0 = VARINT   (int32, int64, uint32, uint64, bool, enum)
 *  1 = FIXED64  (fixed64, sfixed64, double)
 *  2 = DELIMITED (string, bytes, embedded messages, packed repeated)
 *  5 = FIXED32  (fixed32, sfixed32, float)
 *
 * Field tag = (field_number << 3) | wire_type
 */
object ProtobufCodec {

    // ─── Varint helpers ────────────────────────────────────────────────────

    private fun encodeVarint(value: Long): ByteArray {
        val buf = ByteArrayOutputStream()
        var v = value
        while (v ushr 7 != 0L) {
            buf.write(((v and 0x7FL) or 0x80L).toInt())
            v = v ushr 7
        }
        buf.write((v and 0x7FL).toInt())
        return buf.toByteArray()
    }
    private fun readVarint(stream: ByteArrayInputStream): Long {
        var result = 0L
        var shift = 0
        while (true) {
            val b = stream.read()
            if (b == -1) throw IllegalStateException("Unexpected EOF reading varint")
            result = result or ((b.toLong() and 0x7F) shl shift)
            if (b and 0x80 == 0) break
            shift += 7
            if (shift >= 64) throw IllegalStateException("Varint too long")
        }
        return result
    }

    // ─── Field tag helpers ─────────────────────────────────────────────────

    private fun makeTag(fieldNumber: Int, wireType: Int): ByteArray =
        encodeVarint(((fieldNumber shl 3) or wireType).toLong())

    private fun writeField(fieldNumber: Int, wireType: Int, data: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(makeTag(fieldNumber, wireType))
        out.write(data)
        return out.toByteArray()
    }

    private fun writeVarintField(fieldNumber: Int, value: Long): ByteArray =
        writeField(fieldNumber, 0, encodeVarint(value))

    private fun writeBoolField(fieldNumber: Int, value: Boolean): ByteArray =
        writeVarintField(fieldNumber, if (value) 1L else 0L)

    private fun writeUint32Field(fieldNumber: Int, value: Int): ByteArray =
        writeVarintField(fieldNumber, value.toLong() and 0xFFFFFFFFL)

    private fun writeUint64Field(fieldNumber: Int, value: Long): ByteArray =
        writeVarintField(fieldNumber, value)

    private fun writeInt32Field(fieldNumber: Int, value: Int): ByteArray =
        writeVarintField(fieldNumber, value.toLong())

    private fun writeStringField(fieldNumber: Int, value: String): ByteArray {
        if (value.isEmpty()) return byteArrayOf()
        val bytes = value.toByteArray(Charsets.UTF_8)
        val out = ByteArrayOutputStream()
        out.write(makeTag(fieldNumber, 2))
        out.write(encodeVarint(bytes.size.toLong()))
        out.write(bytes)
        return out.toByteArray()
    }

    private fun writeBytesField(fieldNumber: Int, value: ByteArray?): ByteArray {
        if (value == null || value.isEmpty()) return byteArrayOf()
        val out = ByteArrayOutputStream()
        out.write(makeTag(fieldNumber, 2))
        out.write(encodeVarint(value.size.toLong()))
        out.write(value)
        return out.toByteArray()
    }

    // ─── Encode RtmqPack ──────────────────────────────────────────────────

    fun encodeRtmqPack(cmd: Int, seq: Long, data: ByteArray?): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(writeUint32Field(1, cmd))
        if (seq != 0L) out.write(writeUint64Field(2, seq))
        if (data != null && data.isNotEmpty()) out.write(writeBytesField(3, data))
        return out.toByteArray()
    }

    // ─── Encode inner messages ─────────────────────────────────────────────

    fun encodeConnect(msg: ConnectMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.appid.isNotEmpty()) out.write(writeStringField(2, msg.appid))
        if (msg.cid.isNotEmpty()) out.write(writeStringField(3, msg.cid))
        if (msg.uid.isNotEmpty()) out.write(writeStringField(4, msg.uid))
        if (msg.pwd.isNotEmpty()) out.write(writeStringField(5, msg.pwd))
        if (msg.data != null) out.write(writeBytesField(6, msg.data))
        if (msg.will != null) out.write(writeBytesField(7, msg.will))
        if (msg.signsure.isNotEmpty()) out.write(writeStringField(8, msg.signsure))
        if (msg.version.isNotEmpty()) out.write(writeStringField(9, msg.version))
        out.write(writeUint32Field(10, msg.platform))
        out.write(writeUint32Field(11, msg.heartbeat))
        return out.toByteArray()
    }

    fun encodeDisconnect(msg: DisconnectMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.will != null) out.write(writeBytesField(2, msg.will))
        return out.toByteArray()
    }

    fun encodeBind(msg: BindMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.appid.isNotEmpty()) out.write(writeStringField(2, msg.appid))
        if (msg.cid.isNotEmpty()) out.write(writeStringField(3, msg.cid))
        if (msg.uid.isNotEmpty()) out.write(writeStringField(4, msg.uid))
        if (msg.pwd.isNotEmpty()) out.write(writeStringField(5, msg.pwd))
        if (msg.data != null) out.write(writeBytesField(6, msg.data))
        if (msg.will != null) out.write(writeBytesField(7, msg.will))
        return out.toByteArray()
    }

    fun encodeUnbind(msg: UnbindMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.appid.isNotEmpty()) out.write(writeStringField(2, msg.appid))
        if (msg.cid.isNotEmpty()) out.write(writeStringField(3, msg.cid))
        if (msg.uid.isNotEmpty()) out.write(writeStringField(4, msg.uid))
        if (msg.will != null) out.write(writeBytesField(5, msg.will))
        return out.toByteArray()
    }

    fun encodePush(msg: PushMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.topic.isNotEmpty()) out.write(writeStringField(2, msg.topic))
        out.write(writeBoolField(3, msg.ack))
        if (msg.data != null) out.write(writeBytesField(4, msg.data))
        if (msg.extend != null) out.write(writeBytesField(5, msg.extend))
        return out.toByteArray()
    }

    fun encodePushAck(msg: PushAckMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.topic.isNotEmpty()) out.write(writeStringField(2, msg.topic))
        if (msg.extend != null) out.write(writeBytesField(3, msg.extend))
        out.write(writeInt32Field(4, msg.code))
        if (msg.message.isNotEmpty()) out.write(writeStringField(5, msg.message))
        return out.toByteArray()
    }

    fun encodeCall(msg: CallMsg): ByteArray {
        val out = ByteArrayOutputStream()
        if (msg.traceid.isNotEmpty()) out.write(writeStringField(1, msg.traceid))
        if (msg.topic.isNotEmpty()) out.write(writeStringField(2, msg.topic))
        if (msg.data != null) out.write(writeBytesField(3, msg.data))
        if (msg.extend != null) out.write(writeBytesField(4, msg.extend))
        return out.toByteArray()
    }

    // ─── Decode ────────────────────────────────────────────────────────────

    private data class ParsedField(
        val fieldNumber: Int,
        val wireType: Int,
        val varint: Long = 0,
        val bytes: ByteArray = byteArrayOf()
    )

    private fun parseMessage(bytes: ByteArray): List<ParsedField> {
        val fields = mutableListOf<ParsedField>()
        val stream = ByteArrayInputStream(bytes)
        while (stream.available() > 0) {
            val tag = readVarint(stream).toInt()
            val fieldNumber = tag ushr 3
            val wireType = tag and 0x7
            when (wireType) {
                0 -> { // VARINT
                    val v = readVarint(stream)
                    fields.add(ParsedField(fieldNumber, wireType, varint = v))
                }
                2 -> { // DELIMITED
                    val len = readVarint(stream).toInt()
                    val data = ByteArray(len)
                    stream.read(data)
                    fields.add(ParsedField(fieldNumber, wireType, bytes = data))
                }
                1 -> { // FIXED64
                    val data = ByteArray(8)
                    stream.read(data)
                    fields.add(ParsedField(fieldNumber, wireType, bytes = data))
                }
                5 -> { // FIXED32
                    val data = ByteArray(4)
                    stream.read(data)
                    fields.add(ParsedField(fieldNumber, wireType, bytes = data))
                }
                else -> break // unknown wire type, stop parsing
            }
        }
        return fields
    }

    fun decodeRtmqPack(bytes: ByteArray): RtmqPack {
        val fields = parseMessage(bytes)
        var cmd = 0; var seq = 0L; var data: ByteArray? = null; var extend: ByteArray? = null
        for (f in fields) when (f.fieldNumber) {
            1 -> cmd = f.varint.toInt()
            2 -> seq = f.varint
            3 -> data = f.bytes
            4 -> extend = f.bytes
        }
        return RtmqPack(cmd, seq, data, extend)
    }

    fun decodeConnectAck(bytes: ByteArray): ConnectAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var appid = ""; var cid = ""; var uid = ""
        var token = ""; var data: ByteArray? = null; var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> appid   = String(f.bytes, Charsets.UTF_8)
            3 -> cid     = String(f.bytes, Charsets.UTF_8)
            4 -> uid     = String(f.bytes, Charsets.UTF_8)
            5 -> token   = String(f.bytes, Charsets.UTF_8)
            6 -> data    = f.bytes
            7 -> code    = f.varint.toInt()
            8 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return ConnectAckMsg(traceid, appid, cid, uid, token, data, code, message)
    }

    fun decodeDisconnectAck(bytes: ByteArray): DisconnectAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> code    = f.varint.toInt()
            3 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return DisconnectAckMsg(traceid, code, message)
    }

    fun decodeBindAck(bytes: ByteArray): BindAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var appid = ""; var cid = ""; var uid = ""
        var data: ByteArray? = null; var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> appid   = String(f.bytes, Charsets.UTF_8)
            3 -> cid     = String(f.bytes, Charsets.UTF_8)
            4 -> uid     = String(f.bytes, Charsets.UTF_8)
            5 -> data    = f.bytes
            6 -> code    = f.varint.toInt()
            7 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return BindAckMsg(traceid, appid, cid, uid, data, code, message)
    }

    fun decodeUnbindAck(bytes: ByteArray): UnbindAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var appid = ""; var cid = ""; var uid = ""
        var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> appid   = String(f.bytes, Charsets.UTF_8)
            3 -> cid     = String(f.bytes, Charsets.UTF_8)
            4 -> uid     = String(f.bytes, Charsets.UTF_8)
            5 -> code    = f.varint.toInt()
            6 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return UnbindAckMsg(traceid, appid, cid, uid, code, message)
    }

    fun decodePush(bytes: ByteArray): PushMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var topic = ""; var ack = false
        var data: ByteArray? = null; var extend: ByteArray? = null
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> topic   = String(f.bytes, Charsets.UTF_8)
            3 -> ack     = f.varint != 0L
            4 -> data    = f.bytes
            5 -> extend  = f.bytes
        }
        return PushMsg(traceid, topic, ack, data, extend)
    }

    fun decodePushAck(bytes: ByteArray): PushAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var topic = ""; var extend: ByteArray? = null
        var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> topic   = String(f.bytes, Charsets.UTF_8)
            3 -> extend  = f.bytes
            4 -> code    = f.varint.toInt()
            5 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return PushAckMsg(traceid, topic, extend, code, message)
    }

    fun decodeCallAck(bytes: ByteArray): CallAckMsg {
        val fields = parseMessage(bytes)
        var traceid = ""; var topic = ""; var data: ByteArray? = null
        var extend: ByteArray? = null; var code = 0; var message = ""
        for (f in fields) when (f.fieldNumber) {
            1 -> traceid = String(f.bytes, Charsets.UTF_8)
            2 -> topic   = String(f.bytes, Charsets.UTF_8)
            3 -> data    = f.bytes
            4 -> extend  = f.bytes
            5 -> code    = f.varint.toInt()
            6 -> message = String(f.bytes, Charsets.UTF_8)
        }
        return CallAckMsg(traceid, topic, data, extend, code, message)
    }
}