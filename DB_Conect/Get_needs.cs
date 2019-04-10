using System;
using Npgsql;
using System.Threading.Tasks;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using System.Data;

namespace DB_Conect
{
    /// <summary>
    /// Gets informations about active customer orders and store it into Postegresql
    /// </summary> 
    class Get_needs
    {
        static readonly string Str_oracle_conn = Oracle_conn.Connection_string;
        private readonly DateTime start = Loger.Serw_run;
        readonly string npC = Postegresql_conn.Conn_set.ToString();
        public async Task<int> CUST_ord()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='cust_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("START cust_ord " + (DateTime.Now - start));
                // Utwórz połączenie z ORACLE
                List<Orders_row> cust_ord = new List<Orders_row>();
                Dictionary<string, int> D_columns= new Dictionary<string, int>();
                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                {

                    await conO.OpenAsync();
                    OracleGlobalization info = conO.GetSessionInfo();
                    info.DateFormat = "YYYY-MM-DD";
                    conO.SetSessionInfo(info);
                    bool list_columns = false;                    
                    using (OracleCommand cust = new OracleCommand("" +
                        "SELECT ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO) KOOR,a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO,a.CUSTOMER_PO_LINE_NO," +
                        "a.C_DIMENSIONS dimmension,To_Date(c.dat,Decode(InStr(c.dat,'-'),0,'YY/MM/DD','YYYY-MM-DD'))-Delivery_Leadtime Last_Mail_CONF," +
                        "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO) STATe_conf,a.STATE LINE_STATE,ifsapp.customer_order_api.Get_State(a.ORDER_NO) CUST_ORDER_STATE," +
                        "ifsapp.customer_order_api.Get_Country_Code(a.ORDER_NO) Country,ifsapp.customer_order_api.Get_Customer_No(a.ORDER_NO) CUST_no," +
                        "ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO) ZIP_CODE," +
                        "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>') ADDR1," +
                        "Promised_Delivery_Date-Delivery_Leadtime PROM_DATE,To_Char(Promised_Delivery_Date-Delivery_Leadtime,'IYYYIW') PROM_WEEK,LOAD_ID," +
                        "ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID) SHIP_DATE,nvl(a.PART_NO,a.CATALOG_NO) PART_NO," +
                        "nvl(ifsapp.inventory_part_api.Get_Description(CONTRACT,a.PART_NO),a.CATALOG_DESC) Descr,a.CONFIGURATION_ID,a.BUY_QTY_DUE,a.DESIRED_QTY," +
                        "a.QTY_INVOICED,a.QTY_SHIPPED,a.QTY_ASSIGNED,a.DOP_CONNECTION_DB,nvl(b.dop_id,a.Pre_Accounting_Id) dop_id," +
                        "ifsapp.dop_head_api.Get_Objstate__(b.dop_id) DOP_STATE,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1),decode(a.DOP_CONNECTION_DB,NULL,a.PLANNED_DUE_DATE)) Data_dop," +
                        "b.PEGGED_QTY DOP_QTY," +
                        "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0," +
                            "Decode(Nvl(LENGTH(TRIM(TRANSLATE(SubStr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id)," +
                                "instr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),'-')+1), ' +-.0123456789',' '))),1000),1000,b.PEGGED_QTY,0)),b.QTY_DELIVERED) DOP_MADE," +
                        "Nvl(b.CREATE_DATE,decode(a.DOP_CONNECTION_DB,NULL,a.DATE_ENTERED)) DATE_ENTERED," +
                        "owa_opt_lock.checksum(a.OBJVERSION||b.OBJVERSION||nvl(b.dop_id,a.Pre_Accounting_Id)||ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO)||c.dat||" +
                            "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO)||ifsapp.customer_order_api.Get_State(a.ORDER_NO)||ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO)||" +
                            "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||" +
                            "ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>')||load_id||ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID)||ifsapp.dop_head_api.Get_Objstate__(b.dop_id)||" +
                            "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0,b.PEGGED_QTY),b.QTY_DELIVERED)||" +
                            "ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1)) chksum," +
                         "a.Pre_Accounting_Id custID,null zest,ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Flag_Db(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Seria0," +
                         "ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Date(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Data0 " +
                         "FROM " +
                            "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO ID,a.* " +
                            "from  " +
                                "ifsapp.customer_order_line a " +
                            "WHERE  a.OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered') ) a " +
                            "left JOIN " +
                            "ifsapp.dop_demand_cust_ord b " +
                            "ON b.ORDER_NO||'_'||b.LINE_NO||'_'||b.REL_NO||'_'||b.LINE_ITEM_NO=a.id " +
                            "left JOIN " +
                            "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO id," +
                            "SubStr(Decode(SubStr(a.MESSAGE_TEXT,-1,1),']',a.MESSAGE_TEXT,a.MESSAGE_TEXT||']'),Decode(InStr(a.message_text,'/',-10,2),0,-11,-9)," +
                                    "Decode(InStr(a.message_text,'/',-10,2),0,10,8)) DAT " +
                            "FROM " +
                                "ifsapp.customer_order_line_hist a," +
                                "(SELECT Max(HISTORY_NO) hi,a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO  " +
                                "FROM " +
                                    "ifsapp.customer_order_line_hist a," +
                                    "(SELECT order_no FROM ifsapp.customer_order where OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered'))b  " +
                                "WHERE a.order_no=b.order_no AND SubStr(MESSAGE_TEXT,1,3)='Wys'" +
                                "GROUP BY a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO) b " +
                                "WHERE a.HISTORY_NO=b.HI) c  " +
                             "ON c.id=a.id", conO))
                        
                    using (var reader = await cust.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                    {                        
                        while (await reader.ReadAsync())
                        {
                            if(!list_columns)
                            {
                                for(int col=0;col<reader.FieldCount; col++)
                                {
                                    D_columns.Add(reader.GetName(col).ToLower(), col);
                                }
                                list_columns = true;
                            }
                            var rw = new Orders_row
                            {
                                Koor = reader.GetString(D_columns["koor"]),
                                Order_no = reader.GetString(D_columns["order_no"]),
                                Line_no = reader.GetString(D_columns["line_no"]),
                                Rel_no = reader.GetString(D_columns["rel_no"]),
                                Line_item_no = reader.GetInt32(D_columns["line_item_no"]),
                                Customer_po_line_no = reader.GetString(D_columns["customer_po_line_no"]),
                                Dimmension = reader.GetDouble(D_columns["dimmension"]),
                                Last_mail_conf = reader.GetDateTime(D_columns["last_mail_conf"]),
                                State_conf = reader.GetString(D_columns["state_conf"]),
                                Line_state= reader.GetString(D_columns["line_state"]),
                                Cust_order_state= reader.GetString(D_columns["line_state"]),
                                Country= reader.GetString(D_columns["country"]),
                                Cust_no= reader.GetString(D_columns["cust_no"]),
                                Zip_code= reader.GetString(D_columns["zip_code"]),
                                Addr1= reader.GetString(D_columns["addr1"]),
                                Prom_date=reader.GetDateTime(D_columns["prom_date"]),
                                Prom_week= reader.GetString(D_columns["prom_week"]),
                                Load_id= reader.GetInt32(D_columns["load_id"]),
                            };
                        }
                    }
                }
            }
            catch
            {

            }
        }


        /// <summary>
        /// Row Structure in Cutomer_orders table
        /// </summary>
        class Orders_row 
        {
            public string Koor { get; set; }
            public string Order_no { get; set; }
            public string Line_no { get; set; }
            public string Rel_no { get; set; }
            public int Line_item_no { get; set; }
            public string Customer_po_line_no { get; set; }
            public double Dimmension { get; set; }
            public DateTime Last_mail_conf { get; set; }
            public string State_conf { get; set; }
            public string Line_state { get; set; }
            public string Cust_order_state { get; set; }
            public string Country { get; set; }
            public string Cust_no { get; set; }
            public string Zip_code { get; set; }
            public string Addr1 { get; set; }
            public DateTime Prom_date { get; set; }
            public string Prom_week { get; set; }
            public int Load_id { get; set; }
            public DateTime Ship_date { get; set; }
            public string Part_no { get; set; }
            public string Descr { get; set; }
            public string Configuration { get; set; }
            public double Buy_qty_due { get; set; }
            public double Desired_qty { get; set; }
            public double Qty_invoiced { get; set; }
            public double Qty_shipped { get; set; }
            public double Qty_assigned { get; set; }
            public string Dop_connection_db { get; set; }
            public int Dop_id { get; set; }
            public string Dop_state { get; set; }
            public DateTime Data_dop { get; set; }
            public double Dop_qty { get; set; }
            public double Dop_made { get; set; }
            public DateTime Date_entered { get; set; }
            public int Chksum { get; set; }
            public int Custid { get; set; }
            public string Zest { get; set; }
            public bool Seria0 { get; set; }
            public DateTime Data0 { get; set; }
            public Guid Id { get; set; }
        }
    }
}
