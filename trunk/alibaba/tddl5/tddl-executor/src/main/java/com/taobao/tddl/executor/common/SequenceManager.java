package com.taobao.tddl.executor.common;

import java.text.MessageFormat;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.rule.utils.StringXmlApplicationContext;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class SequenceManager extends AbstractSequenceManager {

    private final static Logger       logger                 = LoggerFactory.getLogger(SequenceManager.class);
    public final static String        SEQUENCE_PREFIX        = "com.taobao.tddl.sequence.";
    public final static MessageFormat TDDL5_SEQUENCE_DATA_ID = new MessageFormat(SEQUENCE_PREFIX + "{0}");
    private Map<String, Sequence>     sequences              = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    private String                    appName                = null;
    private String                    unitName               = null;
    private String                    sequenceFile           = null;
    private SequenceLoadFromDBManager subManager             = null;
    private ConfigDataHandler         sequenceCdh            = null;

    public SequenceManager(String appName, String unitName, String sequenceFile, SequenceLoadFromDBManager subManager){
        this.appName = appName;
        this.unitName = unitName;
        this.sequenceFile = sequenceFile;
        this.subManager = subManager;
    }

    @Override
    public void doInit() {
        String dataId = null;
        ConfigDataHandlerFactory factory = null;
        // 优先从文件获取
        if (sequenceFile == null) {
            dataId = TDDL5_SEQUENCE_DATA_ID.format(new Object[] { appName });
            factory = ConfigDataHandlerCity.getFactory(appName, unitName);
        } else {
            factory = ConfigDataHandlerCity.getFileFactory(appName);
            dataId = sequenceFile;
        }

        sequenceCdh = factory.getConfigDataHandler(dataId, new SequenceConfigDataListener());
        String data = sequenceCdh.getData();

        if (TStringUtil.isEmpty(data)) {
            logger.warn("sequence is null, dataId is " + dataId);
            return;
        }

        parseSequence(data);
    }

    public void parseSequence(String data) {
        StringXmlApplicationContext context = new StringXmlApplicationContext(data);
        String[] seqNames = context.getBeanNamesForType(GroupSequence.class);

        sequences.clear();
        for (String seqName : seqNames) {
            GroupSequence seq = (GroupSequence) context.getBean(seqName);
            sequences.put(seqName, seq);
        }
    }

    @Override
    public Long nextValue(String seqName, int batchSize) {
        Sequence seq = getSequence(seqName);
        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }

        try {
            return seq.nextValue(batchSize);
        } catch (SequenceException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, e, seqName, e.getMessage());
        }
    }

    @Override
    public Long nextValue(String seqName) {
        Sequence seq = getSequence(seqName);
        if (seq == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE, seqName);
        }

        try {
            return seq.nextValue();
        } catch (SequenceException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_NEXT_VALUE, e, seqName, e.getMessage());
        }
    }

    public class SequenceConfigDataListener implements ConfigDataListener {

        @Override
        public void onDataRecieved(String dataId, String data) {
            if (TStringUtil.isEmpty(data)) {
                logger.warn("sequence is null, dataId is " + dataId);
                return;
            }

            parseSequence(data);
        }

    }

    @Override
    protected void doDestroy() throws TddlException {
        super.doDestroy();
        sequences.clear();
        if (sequenceCdh != null) {
            sequenceCdh.destroy();
        }
    }

    @Override
    protected Sequence getSequence(String name) {
        Sequence seq = this.sequences.get(name);
        if (seq != null) {
            return seq;
        }

        if (this.subManager != null) {
            // 如果老的sequence没有,才会走到sub sequence
            if (!subManager.isInited()) {
                try {
                    subManager.init();
                } catch (Exception ex) {
                    throw new TddlNestableRuntimeException(ex);
                }
            }

            return subManager.getSequence(name);
        }

        return null;
    }

}
