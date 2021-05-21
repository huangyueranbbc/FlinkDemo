package com.hyr.flink.common

/**
 * 基站日志
 *
 * @param sid      基站的id
 * @param callOut  主叫号码
 * @param callIn   被叫号码
 * @param callType 呼叫类型
 * @param callTime 呼叫时间 (毫秒)
 * @param duration 通话时长 （秒）
 */
case class StationLog(sid: String, var callOut: String, var callIn: String, callType: String, callTime: Long, duration: Long)

/**
 * 请求日志
 *
 * @param vistor      调用者
 * @param ip          ip地址
 * @param api         接口名称
 * @param param       参数
 * @param message     日志信息
 * @param requestTime 调用时间
 * @param costTime    请求耗时 ms
 */
case class RequestLog(vistor: String, ip: String, api: String, param: String, message: String, requestTime: Long, costTime: Int)
