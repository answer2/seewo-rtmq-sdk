package com.seewo.rtmq.network

import com.google.gson.Gson
import com.seewo.rtmq.model.*
import com.seewo.rtmq.protocol.RtmqSerializer
import dev.answer.rtmq.Log

private const val TAG = "Controller"

/**
 * Controller — mirrors the JS Controller class.
 *
 * Sits between NetBase (binary protocol) and IM/Push (business logic).
 * Handles serialization of the JSON ApiPack payload inside the topic data field.
 */
class Controller(private val netBase: NetBase = NetBase()) {

    private val gson = Gson()

    // ─── Init & lifecycle ──────────────────────────────────────────────────

    fun init(cfg: NetBaseConfig): Controller {
        // Encode `data` as base64 JSON, inject version

        val authData = cfg.data?.let { json ->
            val map: MutableMap<String, Any?> = try {
                gson.fromJson<MutableMap<String, Any?>>(
                    json,
                    object : com.google.gson.reflect.TypeToken<MutableMap<String, Any?>>() {}.type
                )
            } catch (e: Exception) {
                mutableMapOf()
            }

            map["version"] = RtmqConstants.VERSION

            RtmqSerializer.jsonToBase64(map)
        } ?: RtmqSerializer.jsonToBase64(
            mapOf("version" to RtmqConstants.VERSION)
        )

        val encodedWill = cfg.will?.let { RtmqSerializer.jsonToBase64(it) }

        netBase.init(cfg.copy(data = authData, will = encodedWill))
        return this
    }

    fun start(): Controller {
        netBase.start()
        return this
    }

    fun stop(): Controller {
        netBase.stop()
        return this
    }

    fun getIsConnected() = netBase.getIsConnected()

    fun onConnect(cb: (ConnectReason) -> Unit): Controller {
        netBase.onConnect { Log.d(TAG, "onConnect: $it"); cb(it) }
        return this
    }

    fun onDisconnect(cb: (CloseReason) -> Unit): Controller {
        netBase.onDisconnect { Log.d(TAG, "onDisconnect: $it"); cb(it) }
        return this
    }

    // ─── send (PUSH) ───────────────────────────────────────────────────────

    fun send(
        rtmqData: RtmqData,
        onSuccess: ((RtmqData) -> Unit)? = null,
        onFail: ((Int, String) -> Unit)? = null,
        onTimeout: (() -> Unit)? = null
    ): Controller {
        Log.d(TAG, "send: $rtmqData")
        val serialized = serializeOutgoing(rtmqData)
        netBase.send(serialized, RequestCallbacks(
            onSuccess = { resp ->
                Log.d(TAG, "send success: $resp")
                onSuccess?.invoke(resp)
            },
            onFail = { code, msg, resp ->
                Log.e(TAG, "send fail: $code $msg")
                onFail?.invoke(code, msg)
            },
            onTimeout = { resp ->
                Log.e(TAG, "send timeout")
                onTimeout?.invoke()
            }
        ))
        return this
    }

    // ─── call (CALL) ───────────────────────────────────────────────────────

    fun call(
        rtmqData: RtmqData,
        onSuccess: ((Any?, RtmqData) -> Unit)? = null,
        onFail: ((Int, String, RtmqData) -> Unit)? = null,
        onTimeout: ((RtmqData) -> Unit)? = null
    ): Controller {
        Log.d(TAG, "call: $rtmqData")
        val serialized = serializeOutgoing(rtmqData)
        netBase.call(serialized, RequestCallbacks(
            onSuccess = { resp ->
                val parsed = deserializeIncoming(resp)
                Log.d(TAG, "call success: $parsed")
                val apiPack = parsed.data as? Map<*, *>
                onSuccess?.invoke(apiPack?.get("data"), parsed)
            },
            onFail = { code, msg, resp ->
                val parsed = deserializeIncoming(resp)
                Log.e(TAG, "call fail: $code $msg")
                onFail?.invoke(code, msg, parsed)
            },
            onTimeout = { resp ->
                Log.e(TAG, "call timeout")
                onTimeout?.invoke(resp)
            }
        ))
        return this
    }

    // ─── onRecv ────────────────────────────────────────────────────────────

    fun onRecv(cb: (Any?, RtmqData) -> Unit): Controller {
        netBase.onRecv { raw ->
            val parsed = deserializeIncoming(raw)
            Log.d(TAG, "onRecv: $parsed")
            val apiPack = parsed.data as? Map<*, *>
            cb(apiPack?.get("data"), parsed)
        }
        return this
    }

    // ─── Serialization helpers ─────────────────────────────────────────────

    /**
     * For IM / PUSH topics: encode the `data` (ApiPack JSON) as base64 bytes.
     * Mirrors JS `_sendDataSerialize`.
     */
    private fun serializeOutgoing(data: RtmqData): RtmqData {
        if (data.topic != RtmqConstants.TOPIC_IM && data.topic != RtmqConstants.TOPIC_PUSH) {
            return data
        }
        val jsonBytes = data.data?.let {
            RtmqSerializer.jsonToBase64(it).toByteArray(Charsets.UTF_8)
        }
        return data.copy(data = jsonBytes)
    }

    /**
     * For IM / PUSH topics: decode base64 bytes back to JSON Map.
     * Mirrors JS `_recvDataParse`.
     */
    private fun deserializeIncoming(data: RtmqData): RtmqData {
        if (data.topic != RtmqConstants.TOPIC_IM && data.topic != RtmqConstants.TOPIC_PUSH) {
            return data
        }
        val decoded = (data.data as? ByteArray)?.let { bytes ->
            val base64Str = String(bytes, Charsets.UTF_8)
            RtmqSerializer.base64ToJson(base64Str)?.let { json ->
                try { gson.fromJson(json, Map::class.java) } catch (_: Exception) { null }
            }
        }
        return data.copy(data = decoded)
    }
}