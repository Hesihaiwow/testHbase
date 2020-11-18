package com.hsh.source;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 知心慧学微服务统一返回值
 *
 * @author Live.InPast
 * @date 2018/10/23
 */
@ResponseBody
public class ZSYResult<T> implements Serializable {

    /**
     * 成功
     */
    public static final String SUCCESS = "00";

    /**
     * 通用失败(业务异常)
     */
    public static final String FAIL = "01";

    /**
     * 请求参数错误
     */
    public static final String PARAM_ERROR = "400";

    /**
     * 数据异常
     */
    public static final String CODE_ERROR = "401";

    /**
     * 数据库操作异常
     */
    public static final String DB_ERROR = "402";

    /**
     * 禁止访问异常
     */
    public static final String FORBIDDEN = "403";

    /**
     * 调用外部http接口异常
     */
    public static final String API_ERROR = "407";

    /**
     * 服务异常
     */
    public static final String SERVER_ERROR = "500";

    /**
     * 需要登录
     */
    public static final String NEED_LOGIN = "10000";

    /**
     * 需要重新授权
     */
    public static final String NEED_AUTHORIATION = "10001";

    public final static String ERR_OK_MSG = "执行成功";
    public final static String ERR_FAIL_MSG = "执行失败";
    public static final String ERR_NEED_LOGIN_MSG = "Token验证失败";

    /**
     * 最大整数,超过该数前端将丢失精度
     */
    private static final Long MAX_VALUE_TO_STRING = 9007199254740992L;


    /**
     * 操作码
     * 00 代表成功
     * XX 代表失败
     */
    private String errCode;

    /**
     * 操作说明
     */
    private String errMsg;

    /**
     * 附加数据
     */
    private T data;

    private ZSYResult() {
    }

    private ZSYResult(String errCode) {
        this.errCode = errCode;
    }

    /**
     * 成功
     *
     * @return
     */
    public static ZSYResult success() {
        ZSYResult result = new ZSYResult(SUCCESS);
        result.errMsg = ERR_OK_MSG;
        return result;
    }

    /**
     * 失败
     *
     * @return
     */
    public static ZSYResult fail() {
        ZSYResult result = new ZSYResult(FAIL);
        result.errMsg = ERR_FAIL_MSG;
        return result;
    }

    /**
     * 失败
     *
     * @param errCode 失败码
     * @return
     */
    public static ZSYResult fail(String errCode) {
        ZSYResult result = new ZSYResult(errCode);
        result.errMsg = ERR_FAIL_MSG;
        return result;
    }


    /**
     * 设置返回结果
     *
     * @param errMsg 接口错误提示
     * @return
     */
    public ZSYResult msg(String errMsg) {
        this.errMsg = errMsg;
        return this;
    }

    /**
     * 设置返回数据
     *
     * @return
     */
    public ZSYResult data(T data) {
        this.data = data;
        return this;
    }

    public String getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public T getData() {
        return data;
    }

    public String build() {
        if (Strings.isNullOrEmpty(this.errCode)) {
            this.errCode = FAIL;
            this.errMsg = ERR_FAIL_MSG;
        }
        return JSONObject.toJSONString(this, (ValueFilter) (object, name, value) -> {
                    if (value instanceof Long &&
                            ((Long) value).longValue() > MAX_VALUE_TO_STRING) {
                        return value.toString();
                    }
                    if (value instanceof List) {
                        return ((List) value).stream().map(val -> {
                            if (val instanceof Long &&
                                    ((Long) value).longValue() > MAX_VALUE_TO_STRING) {
                                return val.toString();
                            }
                            return val;
                        }).collect(Collectors.toList());
                    }

                    return value;
                }, SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteNullListAsEmpty,
                SerializerFeature.WriteNullStringAsEmpty);
    }

}
