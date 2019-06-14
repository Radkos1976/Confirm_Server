using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using System.Collections.Concurrent;
using System.Data.Common;

namespace DB_Conect
{
    /// <summary>
    /// Universal class for update dataset from oracle into postegresql tables
    /// </summary>
    public class Update_pstgr_from_Ora<T> where T : class, new()
    {
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
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns,Dictionary<int,Type> P_types)
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
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns,P_types);
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
            return await Get_PSTGR(Sql_ora, Task_name, D_columns, P_columns,P_types);
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
        public Changes_List<T> Changes(List<T> Old_list, List<T> New_list, string[] ID_column, string IntSorted_by,string guid_col)
        {
            Changes_List<T> modyfications = new Changes_List<T>();
            try
            {
                List<T> _operDEl = new List<T>();
                List<T> _operINS = new List<T>();
                List<T> _operMOD = new List<T>();
                int[] ID= new[] { 100000 };
                int srt=1000;
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
        public async Task<int> PSTRG_Changes_to_dataTable(Changes_List<T> _list,string name_table, string guid_col)
        {

        }
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
