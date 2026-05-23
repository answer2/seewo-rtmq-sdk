package com.seewo.rtmq.network

import com.google.gson.Gson
import com.seewo.rtmq.model.*
import dev.answer.rtmq.Log
import java.util.UUID

private const val TAG = "IM"

/**
 * IM — mirrors the JS IM class.
 *
 * Provides high-level messaging APIs:
 *  - login / logout
 *  - createRoom, joinRoom, quitRoom, deleteRoom
 *  - sendSingleMessage, sendGroupMessage, sendRoomMessage
 *  - pullHisSingleMessage, pullHisGroupMessage, pullHisRoomMessage
 *  - createGroup, deleteGroup, getGroupInfo, getGroupMember, etc.
 *  - onMessageRecv, onNoticeRecv
 */
class IM(private val controller: Controller = Controller()) {

    private val gson = Gson()
    private var seqMap = HashMap<String, Long>()   // uid/gid → latest seq_id

    // ─── Init & Lifecycle ──────────────────────────────────────────────────

    fun init(cfg: NetBaseConfig, auth: String): IM {
        // Wrap auth in authInfo format
        val authData = gson.toJson(mapOf("auth" to auth))
        controller.init(cfg.copy(data = authData))
        return this
    }

    fun login(): IM {
        seqMap.clear()
        controller.start()
        return this
    }

    fun logout(): IM {
        controller.stop()
        return this
    }

    fun getIsConnected() = controller.getIsConnected()

    fun onConnect(cb: (ConnectReason) -> Unit): IM {
        controller.onConnect(cb)
        return this
    }

    fun onDisconnect(cb: (CloseReason) -> Unit): IM {
        controller.onDisconnect(cb)
        return this
    }

    // ─── Room APIs ─────────────────────────────────────────────────────────

    fun createRoom(
        params: Map<String, Any?>,
        onSuccess: ((Any?, RtmqData) -> Unit)? = null,
        onFail: ((Int, String, RtmqData) -> Unit)? = null,
        onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("createRoom", params, onSuccess, onFail, onTimeout)

    fun deleteRoom(roomid: String,
                   onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                   onFail: ((Int, String, RtmqData) -> Unit)? = null,
                   onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("deleteRoom", mapOf("roomid" to roomid), onSuccess, onFail, onTimeout)

    fun joinRoom(roomid: String, password: String = "",
                 onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                 onFail: ((Int, String, RtmqData) -> Unit)? = null,
                 onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("joinRoom", mapOf("roomid" to roomid, "password" to password), onSuccess, onFail, onTimeout)

    fun quitRoom(roomid: String,
                 onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                 onFail: ((Int, String, RtmqData) -> Unit)? = null,
                 onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("quitRoom", mapOf("roomid" to roomid), onSuccess, onFail, onTimeout)

    fun getRoomMember(roomid: String,
                      onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                      onFail: ((Int, String, RtmqData) -> Unit)? = null,
                      onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getRoomMember", mapOf("roomid" to roomid.toLong()), onSuccess, onFail, onTimeout)

    fun getRoomMemberSize(roomid: String,
                          onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                          onFail: ((Int, String, RtmqData) -> Unit)? = null,
                          onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getRoomMemberSize", mapOf("roomid" to roomid.toLong()), onSuccess, onFail, onTimeout)

    // ─── Group APIs ────────────────────────────────────────────────────────

    fun createGroup(name: String, maxSize: Int, members: List<String>,
                    onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                    onFail: ((Int, String, RtmqData) -> Unit)? = null,
                    onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("createGroup", mapOf("name" to name, "max_size" to maxSize, "member" to members),
        onSuccess, onFail, onTimeout)

    fun deleteGroup(gid: String,
                    onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                    onFail: ((Int, String, RtmqData) -> Unit)? = null,
                    onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("deleteGroup", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun getGroupInfo(gid: String,
                     onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                     onFail: ((Int, String, RtmqData) -> Unit)? = null,
                     onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getGroupInfo", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun getGroupList(
        onSuccess: ((Any?, RtmqData) -> Unit)? = null,
        onFail: ((Int, String, RtmqData) -> Unit)? = null,
        onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getGroupList", emptyMap(), onSuccess, onFail, onTimeout)

    fun getGroupMember(gid: String,
                       onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                       onFail: ((Int, String, RtmqData) -> Unit)? = null,
                       onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getGroupMember", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun getGroupMemberSize(gid: String,
                           onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                           onFail: ((Int, String, RtmqData) -> Unit)? = null,
                           onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("getGroupMemberSize", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun addGroupMember(gid: String, uids: List<String>,
                       onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                       onFail: ((Int, String, RtmqData) -> Unit)? = null,
                       onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("addGroupMember", mapOf("gid" to gid, "uids" to uids), onSuccess, onFail, onTimeout)

    fun removeGroupMember(gid: String, uids: List<String>,
                          onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                          onFail: ((Int, String, RtmqData) -> Unit)? = null,
                          onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("removeGroupMember", mapOf("gid" to gid, "uids" to uids), onSuccess, onFail, onTimeout)

    fun joinGroup(gid: String,
                  onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                  onFail: ((Int, String, RtmqData) -> Unit)? = null,
                  onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("joinGroup", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun quitGroup(gid: String,
                  onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                  onFail: ((Int, String, RtmqData) -> Unit)? = null,
                  onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("quitGroup", mapOf("gid" to gid), onSuccess, onFail, onTimeout)

    fun setGroupName(gid: String, name: String,
                     onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                     onFail: ((Int, String, RtmqData) -> Unit)? = null,
                     onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("setGroupName", mapOf("gid" to gid, "name" to name), onSuccess, onFail, onTimeout)

    fun setGroupOwner(gid: String, owner: String,
                      onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                      onFail: ((Int, String, RtmqData) -> Unit)? = null,
                      onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("setGroupOwner", mapOf("gid" to gid, "owner" to owner), onSuccess, onFail, onTimeout)

    // ─── Messaging APIs ────────────────────────────────────────────────────

    fun sendSingleMessage(params: Map<String, Any?>,
                          onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                          onFail: ((Int, String, RtmqData) -> Unit)? = null,
                          onTimeout: ((RtmqData) -> Unit)? = null
    ) = chat("singleChat", params, onSuccess, onFail, onTimeout)

    fun sendGroupMessage(params: Map<String, Any?>,
                         onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                         onFail: ((Int, String, RtmqData) -> Unit)? = null,
                         onTimeout: ((RtmqData) -> Unit)? = null
    ) = chat("groupChat", params, onSuccess, onFail, onTimeout)

    fun sendRoomMessage(params: Map<String, Any?>,
                        onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                        onFail: ((Int, String, RtmqData) -> Unit)? = null,
                        onTimeout: ((RtmqData) -> Unit)? = null
    ) = chat("roomChat", params, onSuccess, onFail, onTimeout)

    fun recallMessage(msgid: String,
                      onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                      onFail: ((Int, String, RtmqData) -> Unit)? = null,
                      onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("recallMessage", mapOf("msgid" to msgid), onSuccess, onFail, onTimeout)

    // ─── History pull APIs ─────────────────────────────────────────────────

    fun pullHisSingleMessage(uid: String, seqId: Long, size: Int, msgTypes: List<Int> = listOf(1, 2, 3), order: Int = 0,
                             onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                             onFail: ((Int, String, RtmqData) -> Unit)? = null,
                             onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("pullHisSingleMessage", mapOf(
        "uid" to uid, "seq_id" to seqId, "size" to size, "msg_types" to msgTypes, "order" to order
    ), onSuccess, onFail, onTimeout)

    fun pullHisGroupMessage(gid: String, seqId: Long, size: Int, msgTypes: List<Int> = listOf(1, 2, 3), order: Int = 0,
                            onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                            onFail: ((Int, String, RtmqData) -> Unit)? = null,
                            onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("pullHisGroupMessage", mapOf(
        "gid" to gid.toLong(), "seq_id" to seqId, "size" to size, "msg_types" to msgTypes, "order" to order
    ), onSuccess, onFail, onTimeout)

    fun pullHisRoomMessage(roomid: String, seqId: Long, size: Int, msgTypes: List<Int> = listOf(1, 2, 3), order: Int = 0,
                           onSuccess: ((Any?, RtmqData) -> Unit)? = null,
                           onFail: ((Int, String, RtmqData) -> Unit)? = null,
                           onTimeout: ((RtmqData) -> Unit)? = null
    ) = doCall("pullHisRoomMessage", mapOf(
        "roomid" to roomid.toLong(), "seq_id" to seqId, "size" to size, "msg_types" to msgTypes, "order" to order
    ), onSuccess, onFail, onTimeout)

    // ─── Receive callbacks ─────────────────────────────────────────────────

    fun onMessageRecv(cb: (Map<*, *>, RtmqData) -> Unit): IM {
        controller.onRecv { data, meta ->
            if (meta.topic != RtmqConstants.TOPIC_IM) return@onRecv
            val map = data as? Map<*, *> ?: return@onRecv
            val method = map["method"] as? String ?: return@onRecv
            if (method in listOf("singleChat", "groupChat", "roomChat", "notify", "pullMessageNotify")) {
                cb(map, meta)
            }
        }
        return this
    }

    fun onNoticeRecv(cb: (Map<*, *>, RtmqData) -> Unit): IM {
        controller.onRecv { data, meta ->
            if (meta.topic != RtmqConstants.TOPIC_IM) return@onRecv
            val map = data as? Map<*, *> ?: return@onRecv
            if (map["method"] == "notify") cb(map, meta)
        }
        return this
    }

    // ─── Private helpers ───────────────────────────────────────────────────

    private fun doCall(
        method: String,
        params: Map<String, Any?>,
        onSuccess: ((Any?, RtmqData) -> Unit)?,
        onFail: ((Int, String, RtmqData) -> Unit)?,
        onTimeout: ((RtmqData) -> Unit)?
    ): IM {
        val payload = mapOf(
            "method" to method,
            "time"   to System.currentTimeMillis(),
            "data"   to params
        )
        val rtmqData = RtmqData(
            traceid = UUID.randomUUID().toString(),
            topic   = RtmqConstants.TOPIC_IM,
            data    = payload
        )
        Log.d(TAG, "$method rtmqData=$rtmqData")
        controller.call(rtmqData, onSuccess, onFail, onTimeout)
        return this
    }

    private fun chat(
        chatType: String,
        params: Map<String, Any?>,
        onSuccess: ((Any?, RtmqData) -> Unit)?,
        onFail: ((Int, String, RtmqData) -> Unit)?,
        onTimeout: ((RtmqData) -> Unit)?
    ): IM {
        val targetTypeMap = mapOf("singleChat" to 1, "groupChat" to 2, "roomChat" to 3)
        val payload = mapOf(
            "method" to chatType,
            "time"   to System.currentTimeMillis(),
            "data"   to mapOf(
                "target_appid"       to params["target_appid"],
                "target_id"          to params["target_id"]?.toString(),
                "msg_expire"         to params["msg_expire"],
                "from_platform"      to params["from_platform"],
                "from_id"            to params["from_id"],
                "from_name"          to params["from_name"],
                "target_type"        to targetTypeMap[chatType],
                "msg_type"           to params["msg_type"],
                "msg_notification"   to params["msg_notification"],
                "msg_body"           to params["msg_body"]
            )
        )
        val bodyJson = gson.toJson(params["msg_body"])
        if (bodyJson.toByteArray(Charsets.UTF_8).size > RtmqConstants.MSG_SIZE_LIMIT) {
            onFail?.invoke(-3, "内容过多，超过限制", RtmqData())
            return this
        }
        val rtmqData = RtmqData(
            traceid = params["traceid"] as? String ?: UUID.randomUUID().toString(),
            topic   = RtmqConstants.TOPIC_IM,
            ack     = true,
            data    = payload
        )
        controller.call(rtmqData, onSuccess, onFail, onTimeout)
        return this
    }
}