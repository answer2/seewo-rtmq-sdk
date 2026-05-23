package com.seewo.rtmq.network

import com.seewo.rtmq.model.*
import com.seewo.rtmq.protocol.RtmqSerializer
import dev.answer.rtmq.Log
import okhttp3.*
import okio.ByteString
import okio.ByteString.Companion.toByteString
import java.util.UUID
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

private const val TAG = "NetBase"

/**
 * NetBase — mirrors the JS NetBase class.
 *
 * Responsibilities:
 *  - Manage OkHttp WebSocket lifecycle
 *  - Handle RTMQ binary protocol (CONNECT → CONNECT_ACK, PING/PONG, etc.)
 *  - Sequence numbering, pending-request map (ack map)
 *  - Auto-reconnect with 3-second delay
 *  - Heartbeat timer
 */
class NetBase(private val okHttpClient: OkHttpClient = OkHttpClient()) {

    // ─── State ─────────────────────────────────────────────────────────────

    private var config: NetBaseConfig? = null
    private var commonCoreInfo: MutableMap<String, String?> = mutableMapOf()

    private var webSocket: WebSocket? = null
    private val isConnected   = AtomicBoolean(false)
    private val isUserClose   = AtomicBoolean(false)
    private val isReconnecting = AtomicBoolean(false)
    var isAutoReconnect: Boolean = true

    private val seq = AtomicLong(0L)
    private val lastRecvTime = AtomicLong(System.currentTimeMillis())

    /** Pending ack-requests: seq → PendingRequest */
    private val ackRequests = ConcurrentHashMap<Long, PendingRequest>()

    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private var heartbeatFuture: ScheduledFuture<*>? = null

    // ─── Callbacks registered by upper layers ──────────────────────────────

    private var onConnectCallback: ((ConnectReason) -> Unit)? = null
    private var onDisconnectCallback: ((CloseReason) -> Unit)? = null
    private var onRecvCallback: ((RtmqData) -> Unit)? = null

    private var closeReason: CloseReason = CloseReasons.unknown()
    private var connectReason: ConnectReason? = null

    // ─── Public API ────────────────────────────────────────────────────────

    fun init(cfg: NetBaseConfig): NetBase {
        config = cfg
        isAutoReconnect = cfg.isAutoReconnect
        commonCoreInfo = mutableMapOf(
            "host"    to cfg.host,
            "appid"   to cfg.appid.trim(),
            "cid"     to cfg.cid,
            "uid"     to cfg.uid,
            "pwd"     to cfg.pwd
        )
        return this
    }

    fun start(): NetBase {
        check(webSocket == null) { "WebSocket already connected" }
        isUserClose.set(false)
        connect()
        return this
    }

    fun stop(will: ByteArray? = null): NetBase {
        isUserClose.set(true)
        val ws = webSocket ?: return this
        val cfg = config ?: return this

        val disconnectMsg = DisconnectMsg(
            traceid = UUID.randomUUID().toString(),
            will = will
        )
        sendPacket(CmdCode.DISCONNECT, disconnectMsg)

        // Wait briefly for DISCONNECT_ACK, then force-close
        scheduler.schedule({
            closeSocket(CloseType.USER_CLOSE)
        }, config?.heartbeat?.toLong() ?: 5L, TimeUnit.SECONDS)
        return this
    }

    fun send(rtmqData: RtmqData, callbacks: RequestCallbacks): NetBase {
        val pushMsg = PushMsg(
            traceid = rtmqData.traceid,
            topic   = rtmqData.topic,
            ack     = rtmqData.ack,
            data    = rtmqData.data as? ByteArray,
            extend  = rtmqData.extend
        )
        sendWithAck(CmdCode.PUSH, pushMsg, callbacks)
        return this
    }

    fun call(rtmqData: RtmqData, callbacks: RequestCallbacks): NetBase {
        val callMsg = CallMsg(
            traceid = rtmqData.traceid,
            topic   = rtmqData.topic,
            data    = rtmqData.data as? ByteArray,
            extend  = rtmqData.extend
        )
        sendWithAck(CmdCode.CALL, callMsg, callbacks)
        return this
    }

    fun onConnect(cb: (ConnectReason) -> Unit): NetBase {
        onConnectCallback = cb
        return this
    }

    fun onDisconnect(cb: (CloseReason) -> Unit): NetBase {
        onDisconnectCallback = cb
        return this
    }

    fun onRecv(cb: (RtmqData) -> Unit): NetBase {
        onRecvCallback = cb
        return this
    }

    fun getIsConnected() = isConnected.get()

    // ─── Internal: connect ─────────────────────────────────────────────────

    private fun connect() {
        val cfg = config ?: return
        val request = Request.Builder()
            .url("wss://${cfg.host}")
            .build()

        webSocket = okHttpClient.newWebSocket(request, object : WebSocketListener() {

            override fun onOpen(ws: WebSocket, response: Response) {
                Log.d(TAG, "WebSocket opened")
                val traceId = UUID.randomUUID().toString()
                val connectMsg = ConnectMsg(
                    traceid   = traceId,
                    appid     = cfg.appid.trim(),
                    cid       = cfg.cid,
                    uid       = cfg.uid,
                    pwd       = cfg.pwd,
                    data      = cfg.data?.toByteArray(Charsets.UTF_8),
                    will      = cfg.will?.toByteArray(Charsets.UTF_8),
                    platform  = cfg.platform,
                    heartbeat = cfg.heartbeat,
                )
                sendWithAck(CmdCode.CONNECT, connectMsg, RequestCallbacks(
                    onSuccess = { _ ->
                        Log.d(TAG, "CONNECT_ACK success, reconnecting=${isReconnecting.get()}")
                        isReconnecting.set(false)
                    },
                    onFail = { code, msg, data ->
                        Log.e(TAG, "CONNECT failed: $code $msg")
                        handleConnectFail(code, msg, data.traceid)
                    },
                    onTimeout = { _ ->
                        Log.e(TAG, "CONNECT timeout")
                        closeSocket(CloseType.CONNECT_TIMEOUT)
                    }
                ))
            }

            override fun onMessage(ws: WebSocket, bytes: ByteString) {
                try {
                    val rtmqData = RtmqSerializer.parsePacket(bytes.toByteArray())
                    handleIncoming(rtmqData)
                } catch (e: Exception) {
                    Log.e(TAG, "Failed to parse incoming packet", e)
                }
            }

            override fun onMessage(ws: WebSocket, text: String) {
                Log.w(TAG, "Received text frame (unexpected): $text")
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                Log.e(TAG, "WebSocket error", t)
                webSocket = null
                isConnected.set(false)
                closeReason = CloseReasons.websocketError()
                onDisconnectCallback?.invoke(closeReason)
                reconnect()
            }

            override fun onClosing(ws: WebSocket, code: Int, reason: String) {
                ws.close(1000, null)
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                Log.d(TAG, "WebSocket closed: $code $reason")
                isConnected.set(false)
                stopHeartbeat()
                if (isReconnecting.get()) {
                    closeReason = CloseReasons.reconnectFail()
                }
                onDisconnectCallback?.invoke(closeReason)
                webSocket = null
                if (!isUserClose.get()) reconnect()
            }
        })
    }

    // ─── Internal: incoming packet handler ────────────────────────────────

    private fun handleIncoming(data: RtmqData) {
        Log.ignore(TAG, "← recv cmd=${data.cmd} seq=${data.seq} code=${data.code}")

        // Reset heartbeat timeout on every received packet
        lastRecvTime.set(System.currentTimeMillis())

        // Update common core info from server response
        updateCommonCoreInfo(data)

        // Handle pending ack requests
        val pending = ackRequests.remove(data.seq)
        if (pending != null) {
            pending.cancel()
            if (data.code == 0) {
                pending.callbacks.onSuccess?.invoke(data)
            } else {
                pending.callbacks.onFail?.invoke(data.code, data.message, data)
            }
        }

        // Route by command
        when (CmdCode.fromValue(data.cmd)) {
            CmdCode.CONNECT_ACK -> handleConnectAck(data)
            CmdCode.DISCONNECT_ACK -> handleDisconnectAck(data)
            CmdCode.PUSH -> handlePushReceived(data)
            CmdCode.PONG -> { /* heartbeat reply, handled by timer reset */ }
            else -> { /* CALL_ACK and others go through ackRequests */ }
        }
    }

    private fun handleConnectAck(data: RtmqData) {
        if (data.code != 0) return
        isConnected.set(true)
        startHeartbeat()
        val reason = connectReason ?: ConnectReasons.userConnect()
        connectReason = null
        onConnectCallback?.invoke(reason)
    }

    private fun handleDisconnectAck(data: RtmqData) {
        // Force-close the WebSocket after receiving ack
        closeSocket(CloseType.USER_CLOSE)
    }

    private fun handlePushReceived(data: RtmqData) {
        // Auto-send PushAck if ack=true
        if (data.ack) {
            val ackMsg = PushAckMsg(
                traceid = data.traceid,
                topic   = data.topic,
                extend  = data.extend,
                code    = 0,
                message = ""
            )
            sendPacket(CmdCode.PUSH_ACK, ackMsg, data.seq)
        }
        onRecvCallback?.invoke(data)
    }

    // ─── Internal: send ────────────────────────────────────────────────────

    private fun sendPacket(cmd: CmdCode, payload: Any? = null, useSeq: Long = -1): Long {
        val ws = webSocket ?: run {
            Log.e(TAG, "WebSocket not connected")
            return -1
        }
        val s = if (useSeq >= 0) useSeq else seq.getAndIncrement()
        try {
            val bytes = RtmqSerializer.buildPacket(cmd, s, payload)
            Log.ignore(TAG, "→ send cmd=${cmd.name} seq=$s len=${bytes.size}")
            ws.send(bytes.toByteString())
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send packet", e)
        }
        return s
    }

    private fun sendWithAck(cmd: CmdCode, payload: Any?, callbacks: RequestCallbacks) {
        val ws = webSocket ?: run {
            callbacks.onFail?.invoke(-1, "WebSocket not connected",
                RtmqData(cmd = cmd.value))
            return
        }
        val s = seq.getAndIncrement()
        val timeoutSec = (config?.heartbeat ?: RtmqConstants.DEFAULT_HEARTBEAT).toLong() * 2 +
                RtmqConstants.TIMEOUT.toLong()

        val pending = PendingRequest(callbacks)
        ackRequests[s] = pending

        // Schedule timeout
        pending.timeoutFuture = scheduler.schedule({
            if (ackRequests.remove(s) != null) {
                callbacks.onTimeout?.invoke(RtmqData(cmd = cmd.value, seq = s))
            }
        }, RtmqConstants.TIMEOUT.toLong(), TimeUnit.SECONDS)

        try {
            val bytes = RtmqSerializer.buildPacket(cmd, s, payload)
            Log.ignore(TAG, "→ sendWithAck cmd=${cmd.name} seq=$s")
            ws.send(bytes.toByteString())
        } catch (e: Exception) {
            ackRequests.remove(s)
            pending.cancel()
            callbacks.onFail?.invoke(-1, e.message ?: "Send error", RtmqData(cmd = cmd.value))
        }
    }

    // ─── Internal: heartbeat ───────────────────────────────────────────────

    // Tracks last time any packet was received from server (reset on every incoming frame)

    private fun startHeartbeat() {
        stopHeartbeat()
        val intervalMs = (config?.heartbeat ?: RtmqConstants.DEFAULT_HEARTBEAT) * 1000L
        val timeoutMs  = intervalMs * 2

        lastRecvTime.set(System.currentTimeMillis())

        heartbeatFuture = scheduler.scheduleAtFixedRate({
            val elapsed = System.currentTimeMillis() - lastRecvTime.get()
            if (elapsed > timeoutMs) {
                Log.e(TAG, "Heartbeat timeout (${elapsed}ms since last recv)")
                closeSocket(CloseType.HEARTBEAT_FAIL)
            } else {
                sendPacket(CmdCode.PING)
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS)
    }

    private fun stopHeartbeat() {
        heartbeatFuture?.cancel(false)
        heartbeatFuture = null
    }

    // ─── Internal: reconnect & close ──────────────────────────────────────

    private fun reconnect() {
        if (isUserClose.get() || !isAutoReconnect) return
        Log.d(TAG, "Reconnecting in 3s...")
        isReconnecting.set(true)
        connectReason = ConnectReasons.reconnect()
        scheduler.schedule({
            if (!isUserClose.get()) connect()
        }, 3L, TimeUnit.SECONDS)
    }

    private fun closeSocket(type: CloseType) {
        closeReason = when (type) {
            CloseType.USER_CLOSE      -> CloseReasons.userClose()
            CloseType.CONNECT_TIMEOUT -> CloseReasons.connectTimeout()
            CloseType.HEARTBEAT_FAIL  -> CloseReasons.heartbeatFail()
            CloseType.CONNECT_FAIL    -> closeReason // already set
            CloseType.RECONNECT_FAIL  -> CloseReasons.reconnectFail()
            CloseType.WEBSOCKET_ERROR -> CloseReasons.websocketError()
            CloseType.UNKNOWN         -> CloseReasons.unknown()
        }
        stopHeartbeat()
        webSocket?.close(1000, closeReason.message)
        webSocket = null
        isConnected.set(false)
    }

    private fun handleConnectFail(code: Int, message: String, traceid: String) {
        isUserClose.set(true)
        closeReason = CloseReasons.connectFail(code, message)
        closeSocket(CloseType.CONNECT_FAIL)
    }

    private fun updateCommonCoreInfo(data: RtmqData) {
        data.cid?.let { commonCoreInfo["cid"] = it }
        data.token?.let { commonCoreInfo["token"] = it }
        data.uid?.let { commonCoreInfo["uid"] = it }
    }

    fun destroy() {
        isUserClose.set(true)
        stopHeartbeat()
        ackRequests.values.forEach { it.cancel() }
        ackRequests.clear()
        webSocket?.close(1000, "Destroy")
        webSocket = null
        scheduler.shutdownNow()
    }
}

// ─── PendingRequest ──────────────────────────────────────────────────────────

internal class PendingRequest(val callbacks: RequestCallbacks) {
    var timeoutFuture: ScheduledFuture<*>? = null
    fun cancel() { timeoutFuture?.cancel(false) }
}