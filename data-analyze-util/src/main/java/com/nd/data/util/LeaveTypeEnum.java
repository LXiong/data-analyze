package com.nd.data.util;

import static com.nd.data.util.HbaseTableUtil.*;

/**
 *
 * @author aladdin
 */
public enum LeaveTypeEnum {

    LEAVE01 {
        @Override
        public String getLeaveType() {
            return "leave01";
        }

        @Override
        public int getMaxInterval() {
            return 1;
        }

        @Override
        public byte[] getLeaveField() {
            return LEAVE01_DATE;
        }
    },
    LEAVE07 {
        @Override
        public String getLeaveType() {
            return "leave07";
        }

        @Override
        public int getMaxInterval() {
            return 7;
        }

        @Override
        public byte[] getLeaveField() {
            return LEAVE07_DATE;
        }
    },
    LEAVE14 {
        @Override
        public String getLeaveType() {
            return "leave14";
        }

        @Override
        public int getMaxInterval() {
            return 14;
        }

        @Override
        public byte[] getLeaveField() {
            return LEAVE14_DATE;
        }
    },
    LEAVE30 {
        @Override
        public String getLeaveType() {
            return "leave30";
        }

        @Override
        public int getMaxInterval() {
            return 30;
        }

        @Override
        public byte[] getLeaveField() {
            return LEAVE30_DATE;
        }
    };

    /**
     * 获取流失类型
     *
     * @return
     */
    public abstract String getLeaveType();

    /**
     * 获取最大流失时间间隔
     *
     * @return
     */
    public abstract int getMaxInterval();

    /**
     * 获取流失标志字段
     *
     * @return
     */
    public abstract byte[] getLeaveField();
}
