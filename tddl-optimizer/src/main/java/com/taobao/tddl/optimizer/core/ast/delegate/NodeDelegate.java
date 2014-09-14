package com.taobao.tddl.optimizer.core.ast.delegate;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.CallbackFilter;
import net.sf.cglib.proxy.Dispatcher;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.exception.OptimizerException;

/**
 * 获取{@linkplain ASTNode}的代理对象
 * 
 * <pre>
 * 主要为解决Merge下node的节点共享问题，TableNode经过规则计算会生成多个基本相同的的分库分表节点
 * 而每个节点唯一不同的主要是group/tableName/extra，内存占用和节点树build代价都会比较高. 
 * 使用代理技术，运行过程中实际只有一个KvIndexNode,分库分表生成的节点都是代理到该节点上，同时持有一个shareIndex下标进行group/tableName数据的查找
 * </pre>
 * 
 * @author jianghang 2014-2-26 下午2:32:22
 * @since 5.0.0
 */
public class NodeDelegate<T extends ASTNode> {

    private static Map<String, Class> reponsitory = new ConcurrentHashMap<String, Class>();
    private T                         delegate;
    private int                       shareIndex;
    private Class                     delegateClass;

    public NodeDelegate(T delegate, int shareIndex){
        this.delegate = delegate;
        this.shareIndex = shareIndex;
        this.delegateClass = delegate.getClass();
    }

    public T getProxy() {
        Class proxyClass = getProxy(delegateClass.getName());
        if (proxyClass == null) {
            Enhancer enhancer = new Enhancer();
            if (delegateClass.isInterface()) { // 判断是否为接口，优先进行接口代理可以解决service为final
                enhancer.setInterfaces(new Class[] { delegateClass });
            } else {
                enhancer.setSuperclass(delegateClass);
            }
            enhancer.setCallbackTypes(new Class[] { ProxyDirect.class, ProxyInterceptor.class });
            enhancer.setCallbackFilter(new ProxyRoute());
            proxyClass = enhancer.createClass();
            // 注册proxyClass
            registerProxy(delegateClass.getName(), proxyClass);
        }

        Enhancer.registerCallbacks(proxyClass, new Callback[] { new ProxyDirect(), new ProxyInterceptor() });
        try {
            Object[] _constructorArgs = new Object[0];
            Constructor _constructor = proxyClass.getConstructor(new Class[] {});// 先尝试默认的空构造函数
            return (T) _constructor.newInstance(_constructorArgs);
        } catch (Throwable e) {
            throw new OptimizerException(e);
        } finally {
            // clear thread callbacks to allow them to be gc'd
            Enhancer.registerStaticCallbacks(proxyClass, null);
        }
    }

    class ProxyRoute implements CallbackFilter {

        @Override
        public int accept(Method method) {
            if (method.getAnnotation(ShareDelegate.class) != null) {
                return 1; // 需要被代理，使用ProxyInterceptor
            } else {
                return 0; // 直接转发,使用ProxyDirect
            }
        }

    }

    class ProxyDirect implements Dispatcher {

        public Object loadObject() throws Exception {
            return delegate;
        }

    }

    class ProxyInterceptor implements MethodInterceptor {

        @Override
        public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
            ASTNode node = (ASTNode) obj;
            String name = method.getName();
            if (name.equals("getDataNode")) {
                return node.getDataNode(shareIndex);
            } else if (name.equals("executeOn")) {
                return node.executeOn((String) args[0], shareIndex);
            } else if (name.equals("getExtra")) {
                return node.getExtra(shareIndex);
            } else if (name.equals("setExtra")) {
                return node.setExtra(args[0], shareIndex);
            } else if (name.equals("toDataNodeExecutor")) {
                return node.toDataNodeExecutor(shareIndex);
            } else if (name.equals("getActualTableName")) {
                return ((TableNode) node).getActualTableName(shareIndex);
            } else if (name.equals("setActualTableName")) {
                return ((TableNode) node).setActualTableName((String) args[0], shareIndex);
            } else if (name.equals("toString")) {
                if (args.length == 0) {
                    return node.toString(0, shareIndex);
                } else {
                    return node.toString((Integer) args[0], shareIndex);
                }
            } else {
                throw new OptimizerException("impossible proxy method : " + name);
            }

        }

    }

    /**
     * 如果存在对应的key的ProxyClass就返回，没有则返回null
     * 
     * @param key
     * @return
     */
    public static Class getProxy(String key) {
        return reponsitory.get(key);
    }

    /**
     * 注册对应的proxyClass到仓库中
     * 
     * @param key
     * @param proxyClass
     */
    public static void registerProxy(String key, Class proxyClass) {
        if (!reponsitory.containsKey(key)) { // 避免重复提交
            synchronized (reponsitory) {
                if (!reponsitory.containsKey(key)) {
                    reponsitory.put(key, proxyClass);
                }
            }
        }
    }
}
