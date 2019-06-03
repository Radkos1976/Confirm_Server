using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using System.Threading.Tasks;
using System.Reflection;


namespace DB_Conect
{    
    /// <summary>
    /// Universal class for update dataset from oracle into postegresql tables
    /// </summary>
    public class Update_pstgr_from_Ora <T> where T :class ,new()
    {
        static readonly string Str_oracle_conn = Oracle_conn.Connection_string;
        private readonly DateTime start = Loger.Serw_run;
        readonly string npC = Postegresql_conn.Conn_set.ToString();        
        public async Task <List <T>> Get_Ora (string Sql_ora)
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
                    Dictionary<string,int > D_columns = new Dictionary<string, int>();
                    Dictionary<string, int> P_columns = new Dictionary<string, int>();
                    PropertyInfo[] propertyInfos = typeof(T).GetProperties();
                    int counter = 0;
                    foreach (PropertyInfo p in propertyInfos)
                    {                       
                        P_columns.Add(p.Name.ToLower(),counter);
                        counter++;
                    }                                   
                    using (OracleCommand cust = new OracleCommand(Sql_ora,conO))
                    {                       
                        using (var reader = await cust.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                if (!list_columns)
                                {
                                    for (int col = 0; col < reader.FieldCount; col++)
                                    {
                                        D_columns.Add(reader.GetName(col).ToLower(), col);
                                    }

                                    list_columns = true;
                                }
                                T Row = new T();
                                foreach (PropertyInfo Property in propertyInfos)
                                {
                                    string metod = Property.Name.ToLower();
                                    if (D_columns.ContainsKey(metod))
                                    {
                                        if (!reader.IsDBNull(D_columns[metod]))
                                        {                                                                              
                                            Property.SetValue(Row, Convert.ChangeType(reader.GetValue(D_columns[metod]), Nullable.GetUnderlyingType(Property.PropertyType)?? Property.PropertyType)); 
                                        }                                        
                                    }                                    
                                }
                                Rows.Add(Row);
                            }
                        }
                    }
                }
                return Rows;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='cust_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("Błąd modyfikacji tabeli Cust_ord:" + e);
                return Rows ;
            }
        }
        public async Task<List<T>> Get_Pstgr(string Sql_ora)
        {
            List<T> Rows = new List<T>();
            try
            {
                using (NpgsqlConnection conO = new NpgsqlConnection(Str_oracle_conn))
                {
                    await conO.OpenAsync();                    
                    bool list_columns = false;
                    Dictionary<string, int> D_columns = new Dictionary<string, int>();
                    Dictionary<string, int> P_columns = new Dictionary<string, int>();
                    PropertyInfo[] propertyInfos = typeof(T).GetProperties();
                    int counter = 0;
                    foreach (PropertyInfo p in propertyInfos)
                    {
                        P_columns.Add(p.Name.ToLower(), counter);
                        counter++;
                    }
                    using (NpgsqlCommand cust = new NpgsqlCommand(Sql_ora, conO))
                    {
                        using (var reader = await cust.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                if (!list_columns)
                                {
                                    for (int col = 0; col < reader.FieldCount; col++)
                                    {
                                        D_columns.Add(reader.GetName(col).ToLower(), col);
                                    }

                                    list_columns = true;
                                }
                                T Row = new T();
                                foreach (PropertyInfo Property in propertyInfos)
                                {
                                    string metod = Property.Name.ToLower();
                                    if (D_columns.ContainsKey(metod))
                                    {
                                        if (!reader.IsDBNull(D_columns[metod]))
                                        {
                                            Property.SetValue(Row, Convert.ChangeType(reader.GetValue(D_columns[metod]), Nullable.GetUnderlyingType(Property.PropertyType) ?? Property.PropertyType));
                                        }
                                    }
                                }
                                Rows.Add(Row);
                            }
                        }
                    }
                }
                return Rows;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='cust_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("Błąd modyfikacji tabeli Cust_ord:" + e);
                return Rows;
            }
        }
    } 
    
}
