package com.alibaba.cobar.manager.dao.xml;

import static com.alibaba.cobar.manager.util.ConstantDefine.COBAR_IDS;
import static com.alibaba.cobar.manager.util.ConstantDefine.ID;
import static com.alibaba.cobar.manager.util.ConstantDefine.SCHEMA;
import static com.alibaba.cobar.manager.util.ConstantDefine.SID;
import static com.alibaba.cobar.manager.util.ConstantDefine.WEIGHT;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.InitializingBean;
import org.xmlpull.mxp1.MXParser;
import org.xmlpull.mxp1_serializer.MXSerializer;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.alibaba.cobar.manager.dao.VipDAO;
import com.alibaba.cobar.manager.dataobject.xml.VipDO;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author haiqing.zhuhq 2011-11-20
 */
public class VipDAOImple extends AbstractDAOImple implements VipDAO, InitializingBean {

    private static final Logger               logger  = LoggerFactory.getLogger(VipDAOImple.class);
    private Map<Long, VipDO>                  map;
    private static long                       maxId;
    private static final ReentrantLock        lock    = new ReentrantLock();
    private static final Map<String, Integer> typeMap = new HashMap<String, Integer>();

    static {
        typeMap.put("id", ID);
        typeMap.put("sid", SID);
        typeMap.put("cobarIds", COBAR_IDS);
        typeMap.put("schema", SCHEMA);
        typeMap.put("weight", WEIGHT);
    }

    public VipDAOImple(){
        map = new HashMap<Long, VipDO>();
        xpp = new MXParser();
        xsl = new MXSerializer();
        maxId = Long.MIN_VALUE;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        xmlPath = xmlFileLoader.getFilePath();
        if (null == xmlPath) {
            logger.error("vip xmlpath doesn't set!");
            throw new IllegalArgumentException("vip xmlpath doesn't set!");
        } else {
            xmlPath = new StringBuilder(xmlPath).append("vip.xml").toString();
            read();
        }

    }

    private boolean read() {
        FileInputStream is = null;
        lock.lock();
        try {
            map.clear();
            is = new FileInputStream(xmlPath);
            xpp.setInput(is, "UTF-8");
            while (!(xpp.getEventType() == XmlPullParser.END_TAG && "vips".equals(xpp.getName()))) {
                if (xpp.getEventType() == XmlPullParser.START_TAG && "vip".equals(xpp.getName())) {
                    VipDO vip = read(xpp);
                    if (null == vip) {
                        throw new XmlPullParserException("Vip read error");
                    }
                    maxId = (maxId < vip.getId()) ? vip.getId() : maxId;
                    map.put(vip.getId(), vip);
                }
                xpp.next();
            }
            is.close();
            return true;
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (XmlPullParserException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            maxId = maxId < 0 ? 0 : maxId;
            lock.unlock();
        }
        if (null != is) {
            try {
                is.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return false;
    }

    private VipDO read(XmlPullParser xpp) {
        VipDO vip = new VipDO();
        try {
            while (!(xpp.getEventType() == XmlPullParser.END_TAG && "vip".equals(xpp.getName()))) {
                if (xpp.getEventType() == XmlPullParser.START_TAG && "property".equals(xpp.getName())) {
                    int type = typeMap.get(xpp.getAttributeValue(0).trim());
                    switch (type) {
                        case ID:
                            vip.setId(Long.parseLong(xpp.nextText().trim()));
                            break;
                        case SID:
                            vip.setSid(xpp.nextText().trim());
                            break;
                        case COBAR_IDS:
                            String[] ids = xpp.nextText().trim().split(",");
                            long[] id;
                            if (ids[0] != "") {
                                id = new long[ids.length];
                                for (int i = 0; i < ids.length; i++) {
                                    id[i] = Long.parseLong(ids[i]);
                                }
                            } else {
                                id = EMPTY_LONG_ARRAY;
                            }
                            vip.setCobarIds(id);
                            break;
                        case SCHEMA:
                            vip.setSchema(xpp.nextText().trim());
                            break;
                        case WEIGHT:
                            String[] weights = xpp.nextText().trim().split(",");
                            int[] weight;
                            if (weights[0] != "") {
                                weight = new int[weights.length];
                                for (int i = 0; i < weights.length; i++) {
                                    weight[i] = Integer.parseInt(weights[i]);
                                }
                            } else {
                                weight = EMPTY_INT_ARRAY;
                            }
                            vip.setWeights(weight);
                            break;
                        default:
                            break;
                    }
                }
                xpp.next();
            }
            if (vip.getCobarIds().length != vip.getWeights().length) {
                logger.error("cobarIds length is not equals to weights length!!");
                throw new RuntimeException("reading vip wrong!");
            }
            return vip;
        } catch (NumberFormatException e) {
            logger.error(e.getMessage(), e);
        } catch (XmlPullParserException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean write() {
        FileOutputStream os = null;
        lock.lock();
        try {
            if (!backup(xmlPath)) {
                logger.error("cluster backup fail!");
            }
            os = new FileOutputStream(xmlPath);
            xsl.setOutput(os, "UTF-8");
            xsl.startDocument("UTF-8", null);
            xsl.text("\n");
            xsl.startTag(null, "vips");
            xsl.text("\n");
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, VipDO> entry = (Entry<Long, VipDO>) it.next();
                VipDO vip = entry.getValue();
                if (!write(vip)) {
                    throw new IOException("vip write error!");
                }
            }
            xsl.endTag(null, "vips");
            xsl.endDocument();
            os.close();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
        if (null != os) {
            try {
                os.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return false;
    }

    private boolean write(VipDO vip) {
        try {
            writePrefix(false);
            xsl.startTag(null, "vip");
            xsl.text("\n");
            writeProperty("id", String.valueOf(vip.getId()));
            writeProperty("sid", vip.getSid());
            writeProperty("cobarIds", vip.idsString());
            writeProperty("schema", vip.getSchema());
            writeProperty("weights", vip.weightsString());
            writePrefix(true);
            xsl.endTag(null, "vip");
            xsl.text("\n");
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean deleteVip(String sid) {
        lock.lock();
        try {
            long id = checkSidContain(sid);
            if (-1 == id) {
                logger.error(new StringBuilder("vip to delete doesn't exsit with sid = ").append(sid).toString());
                return false;
            }
            map.remove(id);
            if (!write()) {
                logger.error("Write fail in  delete vip sid!");
                recovery(xmlPath);
                read();
                return false;
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * check sid is contained
     * 
     * @param sid
     * @return if contained , return id; -1 means is not contained;
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private long checkSidContain(String sid) {
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, VipDO> entry = (Entry<Long, VipDO>) it.next();
            VipDO vip = entry.getValue();
            if (vip.getSid().equals(sid)) {
                return vip.getId();
            }
        }
        return -1;
    }

    @Override
    public boolean deleteVip(long id) {
        lock.lock();
        try {
            if (!map.containsKey(id)) {
                logger.error(new StringBuilder("vip to delete doesn't exsit with id = ").append(id).toString());
                return false;
            }
            map.remove(id);
            if (!write()) {
                logger.error("Write fail in  delete vip id!");
                recovery(xmlPath);
                read();
                return false;
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public boolean addVip(VipDO vip) {
        lock.lock();
        try {
            long id = checkSidContain(vip.getSid());
            if (-1 == id) {
                vip.setId(++maxId);
            } else {
                vip.setId(id);
            }

            map.put(vip.getId(), vip);
            if (!write()) {
                logger.error("Write fail in add vip!");
                recovery(xmlPath);
                read();
                return false;
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * check sid is repeat
     * 
     * @return true for not repeat, else for contained
     */
    @Override
    public boolean checkSid(String sid) {
        return -1 == checkSidContain(sid);
    }

    @Override
    public List<VipDO> listAllVipDO() {
        return new ArrayList<VipDO>(map.values());
    }

    @Override
    public VipDO getVipBySid(String sid) {
        long id = checkSidContain(sid);
        if (-1 == id) {
            return null;
        }
        return map.get(id);
    }

    @Override
    public VipDO getVipById(long id) {
        return map.get(id);
    }

}
