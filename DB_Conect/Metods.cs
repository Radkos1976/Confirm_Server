using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using System.Collections.Concurrent;
using System.Data;

namespace DB_Conect
{
    /// <summary>
    /// Universal class for update dataset from oracle into postegresql tables
    /// </summary>
    public class Update_pstgr_from_Ora<T> where T : class, new()
    {
        public Dictionary<string, NpgsqlTypes.NpgsqlDbType> PostegresTyp = new Dictionary<string, NpgsqlTypes.NpgsqlDbType>
        {
            {"name",NpgsqlTypes.NpgsqlDbType.Name },
            {"oidvector",NpgsqlTypes.NpgsqlDbType.Oidvector },
            {"refcursor",NpgsqlTypes.NpgsqlDbType.Refcursor },
            {"char",NpgsqlTypes.NpgsqlDbType.Char },
            {"varchar",NpgsqlTypes.NpgsqlDbType.Varchar },
            {"text_nonbinary",NpgsqlTypes.NpgsqlDbType.Text },
            {"text",NpgsqlTypes.NpgsqlDbType.Text },
            {"bytea",NpgsqlTypes.NpgsqlDbType.Bytea },
            {"bit",NpgsqlTypes.NpgsqlDbType.Bit },
            {"bool",NpgsqlTypes.NpgsqlDbType.Boolean },
            {"int2",NpgsqlTypes.NpgsqlDbType.Smallint },
            {"int4",NpgsqlTypes.NpgsqlDbType.Integer },
            {"int8",NpgsqlTypes.NpgsqlDbType.Bigint },
            {"float4",NpgsqlTypes.NpgsqlDbType.Real },
            {"float8",NpgsqlTypes.NpgsqlDbType.Double },
            {"numeric",NpgsqlTypes.NpgsqlDbType.Numeric },
            {"money",NpgsqlTypes.NpgsqlDbType.Money },
            {"date",NpgsqlTypes.NpgsqlDbType.Date },
            {"timetz",NpgsqlTypes.NpgsqlDbType.TimeTz },
            {"time",NpgsqlTypes.NpgsqlDbType.Time },
            {"timestamptz",NpgsqlTypes.NpgsqlDbType.TimestampTz },
            {"timestamp",NpgsqlTypes.NpgsqlDbType.Timestamp },
            {"point",NpgsqlTypes.NpgsqlDbType.Point },
            {"box",NpgsqlTypes.NpgsqlDbType.Box },
            {"lseg",NpgsqlTypes.NpgsqlDbType.LSeg },
            {"path",NpgsqlTypes.NpgsqlDbType.Path },
            {"polygon",NpgsqlTypes.NpgsqlDbType.Polygon },
            {"circle",NpgsqlTypes.NpgsqlDbType.Circle },
            {"inet",NpgsqlTypes.NpgsqlDbType.Inet },
            {"macaddr",NpgsqlTypes.NpgsqlDbType.MacAddr },
            {"uuid",NpgsqlTypes.NpgsqlDbType.Uuid },
            {"xml",NpgsqlTypes.NpgsqlDbType.Xml },
            {"interval",NpgsqlTypes.NpgsqlDbType.Interval }
        };
        static readonly string Str_oracle_conn = Oracle_conn.Connection_string;
        private readonly DateTime start = Loger.Serw_run;
        readonly string npC = Postegresql_conn.Conn_set.ToString();
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is diferent  
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types)
        {
            List<T> Rows = new List<T>();
            try
            {
                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                {
                    await conO.OpenAsync();
                    OracleGlobalization info = conO.GetSessionInfo();
                    info.DateFormat = "YYYY-MM-DD";
                    conO.SetSessionInfo(info);
                    bool list_columns = false;
                    T Row = new T();
                    IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                    .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                    using (OracleCommand cust = new OracleCommand(Sql_ora, conO))
                    {
                        using (OracleDataReader reader = cust.ExecuteReader())
                        {
                            reader.FetchSize = cust.RowSize * 200;
                            while (await reader.ReadAsync())
                            {
                                if (!list_columns)
                                {
                                    if (D_columns.Count == 0)
                                    {
                                        for (int col = 0; col < reader.FieldCount; col++)
                                        {
                                            string nam = reader.GetName(col).ToLower();
                                            D_columns.Add(nam, col);
                                        }
                                    }
                                    list_columns = true;
                                }
                                Row = new T();
                                int counter = 0;
                                foreach (var Accessor in Accessors)
                                {
                                    string metod = P_columns[counter];
                                    if (D_columns.ContainsKey(metod))
                                    {
                                        int col = D_columns[metod];
                                        object readData = reader.GetValue(D_columns[metod]);
                                        if (readData != System.DBNull.Value)
                                        {
                                            Type pt = P_types[counter];
                                            Accessor.SetValue(Row, Convert.ChangeType(readData, Nullable.GetUnderlyingType(pt) ?? pt, null));
                                        }
                                    }
                                    counter++;
                                }
                                Rows.Add(Row);
                            }
                        }
                    }
                }
                Rows.Sort();
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log("Błąd modyfikacji tabeli:" + Task_name + e);
                return Rows;
            }
        }
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is same  
        /// </summary>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name)
        {
            Dictionary<string, int> D_columns = new Dictionary<string, int>();
            Dictionary<int, string> P_columns = new Dictionary<int, string>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(counter, p.PropertyInfo.Name.ToLower());
                counter++;
            }
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns, P_types);
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is diferent  
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types)
        {
            List<T> Rows = new List<T>();
            try
            {
                using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                {
                    await conO.OpenAsync();
                    bool list_columns = false;
                    T Row = new T();
                    IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                    .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                    using (NpgsqlCommand cust = new NpgsqlCommand(Sql_ora, conO))
                    {
                        using (NpgsqlDataReader reader = cust.ExecuteReader())
                        {
                            while (await reader.ReadAsync())
                            {
                                if (!list_columns)
                                {
                                    if (D_columns.Count == 0)
                                    {
                                        for (int col = 0; col < reader.FieldCount; col++)
                                        {
                                            D_columns.Add(reader.GetName(col).ToLower(), col);
                                        }
                                    }
                                    list_columns = true;
                                }
                                Row = new T();
                                int counter = 0;
                                foreach (var Accessor in Accessors)
                                {
                                    string metod = P_columns[counter];
                                    if (D_columns.ContainsKey(metod))
                                    {
                                        int col = D_columns[metod];
                                        object readData = reader.GetValue(D_columns[metod]);
                                        if (readData != System.DBNull.Value)
                                        {
                                            Type pt = P_types[counter];
                                            Accessor.SetValue(Row, Convert.ChangeType(readData, Nullable.GetUnderlyingType(pt) ?? pt, null));
                                        }
                                    }
                                    counter++;
                                }
                                Rows.Add(Row);
                            }
                        }
                    }
                }
                Rows.Sort();                
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log("Błąd modyfikacji tabeli:" + Task_name + e);
                return Rows;
            }
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is same
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name)
        {
            Dictionary<string, int> D_columns = new Dictionary<string, int>();
            Dictionary<int, string> P_columns = new Dictionary<int, string>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(counter, p.PropertyInfo.Name.ToLower());
                counter++;
            }
            return await Get_PSTGR(Sql_ora, Task_name, D_columns, P_columns, P_types);
        }
        /// <summary>
        /// Find changes
        /// </summary>
        /// <param name="Old_list"></param>
        /// <param name="New_list"></param>
        /// <param name="ID_column"></param>
        /// <param name="IntSorted_by"></param>
        /// <param name="guid_col"></param>
        /// <returns></returns>
        public Changes_List<T> Changes(List<T> Old_list, List<T> New_list, string[] ID_column, string IntSorted_by, string guid_col)
        {
            Changes_List<T> modyfications = new Changes_List<T>();
            try
            {
                List<T> _operDEl = new List<T>();
                List<T> _operINS = new List<T>();
                List<T> _operMOD = new List<T>();
                int[] ID = new[] { 100000 };
                int srt = 1000;
                int counter = 0;
                int guid_id = 10000;
                Dictionary<int, Type> P_types = new Dictionary<int, Type>();
                T Row = new T();
                IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                   .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();

                foreach (var p in Accessors)
                {
                    string pt_name = p.PropertyInfo.Name.ToLower();
                    if (ID_column.Contains(pt_name)) { ID = (ID ?? Enumerable.Empty<int>()).Concat(Enumerable.Repeat(counter, 1)).ToArray(); }
                    if (pt_name == IntSorted_by.ToLower()) { srt = counter; }
                    if (pt_name == guid_col.ToLower()) { guid_id = counter; }
                    P_types.Add(counter, p.PropertyInfo.PropertyType);
                    counter++;
                }
                counter = 0;
                int max_old_rows = Old_list.Count;
                bool add_Record = false;
                foreach (T rows in New_list)
                {
                    if (max_old_rows > counter)
                    {
                        while (Convert.ToInt32(Accessors[srt].GetValue(rows)) > Convert.ToInt32(Accessors[srt].GetValue(Old_list[counter])))
                        {
                            _operDEl.Add(Old_list[counter]);
                            counter++;
                            if (max_old_rows <= counter) { break; }
                        }
                        if (max_old_rows > counter)
                        {
                            if (Convert.ToInt32(Accessors[srt].GetValue(rows)) == Convert.ToInt32(Accessors[srt].GetValue(Old_list[counter])))
                            {
                                bool changed = false;
                                int col = 0;
                                foreach (var rw in Accessors)
                                {
                                    if (!ID.Contains(col))
                                    {
                                        Type pt = P_types[col];
                                        var val1 = rw.GetValue(rows) == null ? null : Convert.ChangeType(rw.GetValue(rows), Nullable.GetUnderlyingType(pt) ?? pt, null);
                                        var val2 = rw.GetValue(Old_list[counter]) == null ? null : Convert.ChangeType(rw.GetValue(Old_list[counter]), Nullable.GetUnderlyingType(pt) ?? pt, null);
                                        if (val1 == null)
                                        {
                                            if (val2 != null)
                                            {
                                                changed = true;
                                            }
                                        }
                                        else
                                        {
                                            if (val2 == null)
                                            {
                                                if (val1 != null)
                                                {
                                                    changed = true;
                                                }
                                            }
                                            else
                                            {
                                                if (!val1.Equals(val2))
                                                {
                                                    changed = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    col++;
                                }
                                if (changed)
                                {
                                    Row = new T();
                                    col = 0;
                                    foreach (var p in Accessors)
                                    {
                                        if (guid_id == col)
                                        {
                                            p.SetValue(Row, Accessors[guid_id].GetValue(Old_list[counter]));
                                        }
                                        else
                                        {
                                            p.SetValue(Row, p.GetValue(rows));
                                        }
                                        col++;
                                    }
                                    _operMOD.Add(Row);
                                }
                                counter++;
                            }
                            else
                            {
                                add_Record = true;
                            }
                        }
                        else
                        {
                            add_Record = true;
                        }

                    }
                    else
                    {
                        add_Record = true;
                    }
                    if (add_Record)
                    {
                        _operINS.Add(rows);
                        counter++;
                        add_Record = false;
                    }
                }
                var dataset = new Changes_List<T>
                {
                    Insert = _operINS,
                    Delete = _operDEl,
                    Update = _operMOD
                };
                modyfications = dataset;
                return modyfications;

            }
            catch (Exception e)
            {
                Loger.Log("Błąd w procedurze porównania :" + e);
                return modyfications;
            }
        }
        /// <summary>
        /// Return schema of postegrsql table
        /// </summary>
        /// <param name="Table_name"></param>
        /// <returns></returns>
        public async Task<List<Npgsql_Schema_fields>> Get_shema(string Table_name, Dictionary<string, int> P_columns)
        {            
            List <Npgsql_Schema_fields> schema = new List<Npgsql_Schema_fields>();
            using (NpgsqlConnection conO = new NpgsqlConnection(npC))
            {
                await conO.OpenAsync();
                var Tmp = conO.GetSchema("Columns", new string[] { null, null, Table_name });
                foreach (DataRow row in Tmp.Rows)
                {
                    Npgsql_Schema_fields rw = new Npgsql_Schema_fields
                    {
                        Field_name = row["column_name"].ToString(),
                        DB_Col_number = Convert.ToInt32(row["ordinal_position"]) - 1,
                        Field_type = PostegresTyp[row["data_type"].ToString()],
                        Dtst_col = P_columns.ContainsKey(row["column_name"].ToString().ToLower()) ? P_columns[row["column_name"].ToString().ToLower()] : 10000                 
                    };
                    schema.Add(rw);
                }
            }
            return schema;
        }
        public async Task<int> PSTRG_Changes_to_dataTable(Changes_List<T> _list, string name_table, string guid_col)
        {
            Dictionary<string, int> P_columns = new Dictionary<string, int>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(p.PropertyInfo.Name.ToLower(), counter);
                counter++;
            }
            List<Npgsql_Schema_fields> Schema = await Get_shema(name_table, P_columns);
            using (NpgsqlConnection conO = new NpgsqlConnection(npC))
            {
                await conO.OpenAsync();
                using (NpgsqlTransaction npgsqlTransaction = conO.BeginTransaction())
                {
                    if (_list.Delete.Count > 0)
                    {
                        using (NpgsqlCommand cmd = Del_npgsqlCommand(name_table, guid_col, Schema))
                        {
                            cmd.Connection = conO;
                            foreach (T row in _list.Delete)
                            {
                                cmd.Parameters[0].Value = Accessors[P_columns[guid_col]].GetValue(row);
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    if (_list.Update.Count > 0)
                    {

                    }
                }
            }
        }
        
        private NpgsqlCommand Ins_npgsqlCommand(string name_table,  List<Npgsql_Schema_fields> schema_Fields)
        {
            string comand = "INSERT INTO " + name_table;
            string tbl_values="(";
            string param_values = " VALUES (";         
            using (NpgsqlCommand Ins_tmp = new NpgsqlCommand())
            {
                foreach (Npgsql_Schema_fields _Fields in schema_Fields)
                {
                    string nam = _Fields.Field_name;
                    if (_Fields.Dtst_col!=10000)
                    {
                        tbl_values = tbl_values + nam + ",";
                        param_values = param_values + "@" + nam.ToLower() + ",";
                        Ins_tmp.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                    }
                }
                Ins_tmp.CommandText = comand + tbl_values.Substring(0, tbl_values.Length - 1) + ")" + param_values.Substring(0, param_values.Length - 1) + ")";
                Ins_tmp.Prepare();
                return Ins_tmp;
            }
        }        
        private NpgsqlCommand Del_npgsqlCommand(string name_table, string guid_col, List<Npgsql_Schema_fields> schema_Fields)
        {
            string comand = "DELETE FROM " + name_table;
            string tbl_values = " WHERE ";
            string param_values = "=";
            using (NpgsqlCommand del_tmp = new NpgsqlCommand())
            {
                foreach (Npgsql_Schema_fields _Fields in schema_Fields)
                {
                    string nam = _Fields.Field_name;
                    if (_Fields.Dtst_col != 10000)
                    {
                        tbl_values = tbl_values + nam;
                        param_values = "@" + nam.ToLower();
                        del_tmp.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                    }
                }
                del_tmp.CommandText = comand + tbl_values + param_values;
                del_tmp.Prepare();
                return del_tmp;
            }
        }
        /// <summary>
        /// Create Npgsql command for Modify
        /// </summary>
        /// <param name="Row"></param>
        /// <param name="name_table"></param>
        /// <param name="guid_col"></param>
        /// <returns></returns>
        private NpgsqlCommand Mod_npgsqlCommand(string name_table, string guid_col, List<Npgsql_Schema_fields> schema_Fields)
        {
            string comand = "UPDATE " + name_table + " SET";
            string tbl_values = " ";
            string param_values = " WHERE ";
            using (NpgsqlCommand mod_tmp = new NpgsqlCommand())
            {
                foreach (Npgsql_Schema_fields _Fields in schema_Fields)
                {
                    string nam = _Fields.Field_name;
                    if (_Fields.Dtst_col != 10000)
                    {
                        if (nam.ToLower()== guid_col.ToLower())
                        {
                            param_values = tbl_values + nam + "=@" + nam.ToLower();
                        }
                        else
                        {
                            tbl_values = tbl_values + nam + "=@" + nam.ToLower() + ",";
                        }                                              
                        mod_tmp.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                    }
                }
                mod_tmp.CommandText = comand + tbl_values.Substring(0, tbl_values.Length - 1) + " " + param_values;
                mod_tmp.Prepare();
                return mod_tmp;
            }
        }
        
    } 
    
    public class Npgsql_Schema_fields
    {
        public string Field_name { get; set; }
        public int DB_Col_number { get; set; }
        public NpgsqlTypes.NpgsqlDbType Field_type { get; set; }
        public int Dtst_col { get; set; }       
    }
    public class Changes_List<T> where T : class,new() 
    {
        public List<T> Insert;
        public List<T> Update;
        public List<T> Delete;
    }
    public interface IPropertyAccessor
    {
        PropertyInfo PropertyInfo { get; }
        object GetValue(object source);
        void SetValue(object source, object value);
    }
    public static class PropertyInfoHelper
    {
        private static ConcurrentDictionary<PropertyInfo, IPropertyAccessor> _cache =
            new ConcurrentDictionary<PropertyInfo, IPropertyAccessor>();

        public static IPropertyAccessor GetAccessor(PropertyInfo propertyInfo)
        {
            if (!_cache.TryGetValue(propertyInfo, out IPropertyAccessor result))
            {
                result = CreateAccessor(propertyInfo);
                _cache.TryAdd(propertyInfo, result); ;
            }
            return result;
        }

        public static IPropertyAccessor CreateAccessor(PropertyInfo PropertyInfo)
        {
            var GenType = typeof(PropertyWrapper<,>)
                .MakeGenericType(PropertyInfo.DeclaringType, PropertyInfo.PropertyType);
            return (IPropertyAccessor)Activator.CreateInstance(GenType, PropertyInfo);
        }
    }
    internal class PropertyWrapper<TObject, TValue> : IPropertyAccessor where TObject : class
    {
        private readonly Func<TObject, TValue> Getter;
        private readonly Action<TObject, TValue> Setter;

        public PropertyWrapper(PropertyInfo PropertyInfo)
        {
            this.PropertyInfo = PropertyInfo;

            MethodInfo GetterInfo = PropertyInfo.GetGetMethod(true);
            MethodInfo SetterInfo = PropertyInfo.GetSetMethod(true);

            Getter = (Func<TObject, TValue>)Delegate.CreateDelegate
                    (typeof(Func<TObject, TValue>), GetterInfo);
            Setter = (Action<TObject, TValue>)Delegate.CreateDelegate
                    (typeof(Action<TObject, TValue>), SetterInfo);
        }

        object IPropertyAccessor.GetValue(object source)
        {
            return Getter(source as TObject);
        }

        void IPropertyAccessor.SetValue(object source, object value)
        {
            Setter(source as TObject, (TValue)value);
        }

        public PropertyInfo PropertyInfo { get; private set; }
    }
}
