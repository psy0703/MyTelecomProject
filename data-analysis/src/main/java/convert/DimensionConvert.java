package convert;

import bean.ContactDimension;
import bean.DateDimension;
import bean.IDimension;
import com.ng.dataconsumer.util.JDBCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DimensionConvert {

    private static LRUCache cache = new LRUCache(200);

    /**
     * 根据传入的维度, 去响应的 表中查询到需要的id
     * @param iDimension
     * @return
     */
    public static int getDimensionId(IDimension iDimension) {
        //先从内存读取id，如果读不到 telephone_name
        Integer cacheId = cache.get(iDimension.toString());
        if (cacheId != null) return cacheId;
        //1.得到sql语句
        String[] sqls = getSqls(iDimension);

        //2.执行sql，拿到需要的id
        Connection conn = JDBCUtil.getInstance();
        int id = execSqls(conn,sqls,iDimension);

        //3.添加到内存中
        cache.put(iDimension.toString(), id);
        return  id;
    }

    /**
     * 执行sql ， 返回需要的id
     * @param conn
     * @param sqls
     * @param iDimension
     * @return
     */
    private static int execSqls(Connection conn, String[] sqls, IDimension iDimension) {

        try {
            PreparedStatement ps = conn.prepareStatement(sqls[0]);
            setArguments(ps,iDimension);
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }

            //2.插入
            ps = conn.prepareStatement(sqls[1]);
            setArguments(ps, iDimension);
            ps.executeUpdate();

            /*ResultSet set = ps.getGeneratedKeys();
            return set.getInt(1);*/

            //3.查询
            ps = conn.prepareStatement(sqls[0]);
            setArguments(ps, iDimension);
            resultSet = ps.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1; // 如果前面出现了异常, id将会返回-1
    }

    /**
     * 设置参数
     * @param ps
     * @param iDimension
     * @throws SQLException
     */
    private static void setArguments(PreparedStatement ps, IDimension iDimension) throws SQLException {
        if (iDimension instanceof ContactDimension) {
            ContactDimension cd = (ContactDimension) iDimension;
            ps.setString(1,cd.getTelephone());
            ps.setString(2, cd.getName());
        } else {
            DateDimension dd = (DateDimension) iDimension;
            ps.setInt(1, dd.getYear());
            ps.setInt(2, dd.getMonth());
            ps.setInt(3, dd.getDay());

        }
    }

    /**
     * 获取sql语句:
     * 1. 查询
     * 2. 插入
     * @param iDimension
     * @return
     */
    private static String[] getSqls(IDimension iDimension) {
        String[] sqls = new String[2];
        if (iDimension instanceof ContactDimension) {
            sqls[0] = "select id form tb_contact where telephone=? and name=?";
            sqls[1] = "insert into tb_contacts values(null,?,?)";
        } else{
            sqls[0] = "select id from tb_date where year=? and month=? and day=?";
            sqls[1] = "insert into tb_date values(null,?,?,?)";
        }

        return sqls;

    }


    /**
     * 缓存类, 提高查找速度
     */
    public static class LRUCache extends LinkedHashMap<String, Integer> {
        private static  final long serialVersionUID = 1L;
        protected  int maxElements;

        public LRUCache(int maxSize){
            super(maxSize, 0.75F, true);
            this.maxElements = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return (size() > this.maxElements);
        }
    }

}
