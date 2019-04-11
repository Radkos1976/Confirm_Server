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
                List<Orders_row> Oracust_ord = new List<Orders_row>();
                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                {

                    await conO.OpenAsync();
                    OracleGlobalization info = conO.GetSessionInfo();
                    info.DateFormat = "YYYY-MM-DD";
                    conO.SetSessionInfo(info);
                    bool list_columns = false;
                    Dictionary<string, int> D_columns = new Dictionary<string, int>();
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
                    {
                        using (var reader = await cust.ExecuteReaderAsync(CommandBehavior.CloseConnection))
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
                                Orders_row rw = new Orders_row
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
                                    Line_state = reader.GetString(D_columns["line_state"]),
                                    Cust_order_state = reader.GetString(D_columns["line_state"]),
                                    Country = reader.GetString(D_columns["country"]),
                                    Cust_no = reader.GetString(D_columns["cust_no"]),
                                    Zip_code = reader.GetString(D_columns["zip_code"]),
                                    Addr1 = reader.GetString(D_columns["addr1"]),
                                    Prom_date = reader.GetDateTime(D_columns["prom_date"]),
                                    Prom_week = reader.GetString(D_columns["prom_week"]),
                                    Load_id = reader.GetInt32(D_columns["load_id"]),
                                    Ship_date = reader.GetDateTime(D_columns["ship_date"]),
                                    Part_no = reader.GetString(D_columns["part_no"]),
                                    Descr = reader.GetString(D_columns["descr"]),
                                    Configuration = reader.GetString(D_columns["configuration"]),
                                    Buy_qty_due = reader.GetDouble(D_columns["buy_qty_due"]),
                                    Desired_qty = reader.GetDouble(D_columns["desired_qty"]),
                                    Qty_invoiced = reader.GetDouble(D_columns["qty_invoiced"]),
                                    Qty_shipped = reader.GetDouble(D_columns["qty_shipped"]),
                                    Qty_assigned = reader.GetDouble(D_columns["qty_assigned"]),
                                    Dop_connection_db = reader.GetString(D_columns["dop_connection_db"]),
                                    Dop_id = reader.GetInt32(D_columns["dop_id"]),
                                    Dop_state = reader.GetString(D_columns["dop_state"]),
                                    Data_dop = reader.GetDateTime(D_columns["data_dop"]),
                                    Dop_qty = reader.GetDouble(D_columns["dop_qty"]),
                                    Dop_made = reader.GetDouble(D_columns["dop_made"]),
                                    Date_entered = reader.GetDateTime(D_columns["date_entered"]),
                                    Chksum = reader.GetInt32(D_columns["chksum"]),
                                    Custid = reader.GetInt32(D_columns["custid"]),
                                    Zest = reader.GetString(D_columns["zest"]),
                                    Seria0 = reader.GetBoolean(D_columns["seria0"]),
                                    Data0 = reader.GetDateTime(D_columns["data0"]),
                                    Id = reader.GetGuid(D_columns["id"])
                                };
                                Oracust_ord.Add(rw);
                            }
                        }
                    }
                    Oracust_ord.Sort();
                }
                List<Orders_row> PstgrCust_ord = new List<Orders_row>();
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    bool list_columns = false;
                    Dictionary<string, int> D_columns = new Dictionary<string, int>();
                    await conA.OpenAsync();
                    using (NpgsqlCommand PstgrOrd = new NpgsqlCommand("Select * from cust_ord", conA))
                    {
                        using (var reader = await PstgrOrd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
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
                                Orders_row rw = new Orders_row
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
                                    Line_state = reader.GetString(D_columns["line_state"]),
                                    Cust_order_state = reader.GetString(D_columns["line_state"]),
                                    Country = reader.GetString(D_columns["country"]),
                                    Cust_no = reader.GetString(D_columns["cust_no"]),
                                    Zip_code = reader.GetString(D_columns["zip_code"]),
                                    Addr1 = reader.GetString(D_columns["addr1"]),
                                    Prom_date = reader.GetDateTime(D_columns["prom_date"]),
                                    Prom_week = reader.GetString(D_columns["prom_week"]),
                                    Load_id = reader.GetInt32(D_columns["load_id"]),
                                    Ship_date = reader.GetDateTime(D_columns["ship_date"]),
                                    Part_no = reader.GetString(D_columns["part_no"]),
                                    Descr = reader.GetString(D_columns["descr"]),
                                    Configuration = reader.GetString(D_columns["configuration"]),
                                    Buy_qty_due = reader.GetDouble(D_columns["buy_qty_due"]),
                                    Desired_qty = reader.GetDouble(D_columns["desired_qty"]),
                                    Qty_invoiced = reader.GetDouble(D_columns["qty_invoiced"]),
                                    Qty_shipped = reader.GetDouble(D_columns["qty_shipped"]),
                                    Qty_assigned = reader.GetDouble(D_columns["qty_assigned"]),
                                    Dop_connection_db = reader.GetString(D_columns["dop_connection_db"]),
                                    Dop_id = reader.GetInt32(D_columns["dop_id"]),
                                    Dop_state = reader.GetString(D_columns["dop_state"]),
                                    Data_dop = reader.GetDateTime(D_columns["data_dop"]),
                                    Dop_qty = reader.GetDouble(D_columns["dop_qty"]),
                                    Dop_made = reader.GetDouble(D_columns["dop_made"]),
                                    Date_entered = reader.GetDateTime(D_columns["date_entered"]),
                                    Chksum = reader.GetInt32(D_columns["chksum"]),
                                    Custid = reader.GetInt32(D_columns["custid"]),
                                    Zest = reader.GetString(D_columns["zest"]),
                                    Seria0 = reader.GetBoolean(D_columns["seria0"]),
                                    Data0 = reader.GetDateTime(D_columns["data0"])
                                };
                                PstgrCust_ord.Add(rw);
                            }
                        }
                    }
                    PstgrCust_ord.Sort();
                }
                List<Orders_row> _Row_del = new List<Orders_row>();
                List<Orders_row> _Row_mod = new List<Orders_row>();
                List<Orders_row> _Row_add = new List<Orders_row>();
                Loger.Log("REaDY UPDATE cust_ord " + (DateTime.Now - start));
                int cust_count = 0;
                int max_cust_ord = PstgrCust_ord.Count;
                foreach (Orders_row rek in Oracust_ord)
                {
                    if (max_cust_ord > cust_count)
                    {
                        while (rek.Custid > PstgrCust_ord[cust_count].Custid)
                        {
                            Orders_row rw = new Orders_row
                            {
                                Id = rek.Id,
                                Custid = rek.Custid
                            };
                            _Row_del.Add(rw);
                            cust_count++;
                            if (max_cust_ord <= cust_count) { break; }
                        }
                    }
                    if (max_cust_ord > cust_count)
                    {
                        if (rek.Custid == PstgrCust_ord[cust_count].Custid)
                        {
                            bool updt = false;
                            bool dta0 = false;
                            bool dta1 = false;
                            if (rek.Data0.ToString() == "") { dta0 = true; }
                            if (PstgrCust_ord[cust_count].Data0.ToString() == "") { dta1 = true; }
                            if (rek.Chksum != PstgrCust_ord[cust_count].Chksum | rek.Seria0 != PstgrCust_ord[cust_count].Seria0)
                            {
                                updt = true;
                            }
                            else if (dta0 != dta1)
                            {
                                if (PstgrCust_ord[cust_count].Data0 != rek.Data0)
                                {
                                    updt = true;
                                }
                            }
                            else if (dta0 == false && dta1 == false)
                            {
                                if (PstgrCust_ord[cust_count].Data0.Date != rek.Data0.Date)
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].State_conf.ToString() != rek.State_conf.ToString())
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Last_mail_conf.ToString() != rek.Last_mail_conf.ToString())
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Customer_po_line_no.ToString() != rek.Customer_po_line_no.ToString())
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!PstgrCust_ord[cust_count].Dimmension.HasValue)
                                {
                                    if (rek.Dimmension.HasValue)
                                    {
                                        updt = true;
                                    }
                                }
                                else
                                {
                                    if (!updt && PstgrCust_ord[cust_count].Dimmension != rek.Dimmension)
                                    {
                                        updt = true;
                                    }
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Line_state.ToString() != rek.Line_state.ToString())
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Cust_order_state.ToString() != rek.Cust_order_state.ToString())
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Prom_week != rek.Prom_week)
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Addr1.ToString() != rek.Addr1.ToString())
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Load_id.ToString() != rek.Load_id.ToString())
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Prom_date.Date != rek.Prom_date.Date)
                                {
                                    updt = true;
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Last_mail_conf.ToString() == "")
                                {
                                    if (rek.Last_mail_conf.ToString() != "")
                                    {
                                        updt = true;
                                    }
                                }
                                else
                                {
                                    if (rek.Last_mail_conf.ToString() == "")
                                    {
                                        updt = true;
                                    }
                                    else
                                    {
                                        if (PstgrCust_ord[cust_count].Last_mail_conf != rek.Last_mail_conf.Date)
                                        {
                                            updt = true;
                                        }
                                    }
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Ship_date.ToString() == "" || !PstgrCust_ord[cust_count].Ship_date.HasValue)
                                {
                                    if (rek.Ship_date.ToString() != "")
                                    {
                                        updt = true;
                                    }
                                }
                                else
                                {
                                    if (rek.Ship_date.ToString() == "" || !rek.Ship_date.HasValue)
                                    {
                                        updt = true;
                                    }
                                    else
                                    {
                                        if (Convert.ToDateTime(PstgrCust_ord[cust_count].Ship_date).Date != Convert.ToDateTime(rek.Ship_date).Date)
                                        {
                                            updt = true;
                                        }
                                    }
                                }
                            }
                            if (!updt)
                            {
                                if (!updt && PstgrCust_ord[cust_count].Dop_id != rek.Dop_id)
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Dop_state.ToString() != rek.Dop_state.ToString())
                                {
                                    updt = true;
                                }
                                if (!updt && PstgrCust_ord[cust_count].Data_dop.ToString() == "" || PstgrCust_ord[cust_count].Data_dop.HasValue)
                                {
                                    if (rek.Data_dop.ToString() != "")
                                    {
                                        updt = true;
                                    }
                                }
                                else
                                {
                                    if (rek.Data_dop.ToString() == "" || !rek.Data_dop.HasValue)
                                    {
                                        updt = true;
                                    }
                                    else
                                    {
                                        if (Convert.ToDateTime(PstgrCust_ord[cust_count].Data_dop).Date != Convert.ToDateTime(rek.Data_dop).Date)
                                        {
                                            updt = true;
                                        }
                                    }
                                }
                            }

                            if (updt)
                            {
                                Orders_row rw = new Orders_row
                                {
                                    Koor = rek.Koor,
                                    Order_no = rek.Order_no,
                                    Line_no = rek.Line_no,
                                    Rel_no = rek.Rel_no,
                                    Line_item_no = rek.Line_item_no,
                                    Customer_po_line_no = rek.Customer_po_line_no,
                                    Dimmension = rek.Dimmension,
                                    Last_mail_conf = rek.Last_mail_conf,
                                    State_conf = rek.State_conf,
                                    Line_state = rek.Line_state,
                                    Cust_order_state = rek.Line_state,
                                    Country = rek.Country,
                                    Cust_no = rek.Cust_no,
                                    Zip_code = rek.Zip_code,
                                    Addr1 = rek.Addr1,
                                    Prom_date = rek.Prom_date,
                                    Prom_week = rek.Prom_week,
                                    Load_id = rek.Load_id,
                                    Ship_date = rek.Ship_date,
                                    Part_no = rek.Part_no,
                                    Descr = rek.Descr,
                                    Configuration = rek.Configuration,
                                    Buy_qty_due = rek.Buy_qty_due,
                                    Desired_qty = rek.Desired_qty,
                                    Qty_invoiced = rek.Qty_invoiced,
                                    Qty_shipped = rek.Qty_shipped,
                                    Qty_assigned = rek.Qty_assigned,
                                    Dop_connection_db = rek.Dop_connection_db,
                                    Dop_id = rek.Dop_id,
                                    Dop_state = rek.Dop_state,
                                    Data_dop = rek.Data_dop,
                                    Dop_qty = rek.Dop_qty,
                                    Dop_made = rek.Dop_made,
                                    Date_entered = rek.Date_entered,
                                    Chksum = rek.Chksum,
                                    Custid = rek.Custid,
                                    Zest = rek.Zest,
                                    Seria0 = rek.Seria0,
                                    Data0 = rek.Data0,
                                    Id = PstgrCust_ord[cust_count].Id
                                };
                                _Row_mod.Add(rw);
                            }
                            cust_count++;
                        }
                        else
                        {
                            Orders_row rw = new Orders_row
                            {
                                Koor = rek.Koor,
                                Order_no = rek.Order_no,
                                Line_no = rek.Line_no,
                                Rel_no = rek.Rel_no,
                                Line_item_no = rek.Line_item_no,
                                Customer_po_line_no = rek.Customer_po_line_no,
                                Dimmension = rek.Dimmension,
                                Last_mail_conf = rek.Last_mail_conf,
                                State_conf = rek.State_conf,
                                Line_state = rek.Line_state,
                                Cust_order_state = rek.Line_state,
                                Country = rek.Country,
                                Cust_no = rek.Cust_no,
                                Zip_code = rek.Zip_code,
                                Addr1 = rek.Addr1,
                                Prom_date = rek.Prom_date,
                                Prom_week = rek.Prom_week,
                                Load_id = rek.Load_id,
                                Ship_date = rek.Ship_date,
                                Part_no = rek.Part_no,
                                Descr = rek.Descr,
                                Configuration = rek.Configuration,
                                Buy_qty_due = rek.Buy_qty_due,
                                Desired_qty = rek.Desired_qty,
                                Qty_invoiced = rek.Qty_invoiced,
                                Qty_shipped = rek.Qty_shipped,
                                Qty_assigned = rek.Qty_assigned,
                                Dop_connection_db = rek.Dop_connection_db,
                                Dop_id = rek.Dop_id,
                                Dop_state = rek.Dop_state,
                                Data_dop = rek.Data_dop,
                                Dop_qty = rek.Dop_qty,
                                Dop_made = rek.Dop_made,
                                Date_entered = rek.Date_entered,
                                Chksum = rek.Chksum,
                                Custid = rek.Custid,
                                Zest = rek.Zest,
                                Seria0 = rek.Seria0,
                                Data0 = rek.Data0,
                                Id = Guid.NewGuid()
                            };
                            _Row_add.Add(rw);
                        }
                    }
                    else
                    {
                        Orders_row rw = new Orders_row
                        {
                            Koor = rek.Koor,
                            Order_no = rek.Order_no,
                            Line_no = rek.Line_no,
                            Rel_no = rek.Rel_no,
                            Line_item_no = rek.Line_item_no,
                            Customer_po_line_no = rek.Customer_po_line_no,
                            Dimmension = rek.Dimmension,
                            Last_mail_conf = rek.Last_mail_conf,
                            State_conf = rek.State_conf,
                            Line_state = rek.Line_state,
                            Cust_order_state = rek.Line_state,
                            Country = rek.Country,
                            Cust_no = rek.Cust_no,
                            Zip_code = rek.Zip_code,
                            Addr1 = rek.Addr1,
                            Prom_date = rek.Prom_date,
                            Prom_week = rek.Prom_week,
                            Load_id = rek.Load_id,
                            Ship_date = rek.Ship_date,
                            Part_no = rek.Part_no,
                            Descr = rek.Descr,
                            Configuration = rek.Configuration,
                            Buy_qty_due = rek.Buy_qty_due,
                            Desired_qty = rek.Desired_qty,
                            Qty_invoiced = rek.Qty_invoiced,
                            Qty_shipped = rek.Qty_shipped,
                            Qty_assigned = rek.Qty_assigned,
                            Dop_connection_db = rek.Dop_connection_db,
                            Dop_id = rek.Dop_id,
                            Dop_state = rek.Dop_state,
                            Data_dop = rek.Data_dop,
                            Dop_qty = rek.Dop_qty,
                            Dop_made = rek.Dop_made,
                            Date_entered = rek.Date_entered,
                            Chksum = rek.Chksum,
                            Custid = rek.Custid,
                            Zest = rek.Zest,
                            Seria0 = rek.Seria0,
                            Data0 = rek.Data0,
                            Id = Guid.NewGuid()
                        };
                        _Row_add.Add(rw);
                    }
                }
                PstgrCust_ord = null;
                Oracust_ord = null;
                int c_add = _Row_add.Count;
                int c_del = _Row_del.Count;
                int c_mod = _Row_mod.Count;
                if (c_add + c_del + c_mod > 0)
                {
                    using (NpgsqlConnection conB = new NpgsqlConnection(npC))
                    {
                        Loger.Log("START UPDATE_cust: " + (DateTime.Now - start));
                        await conB.OpenAsync();
                        using (NpgsqlTransaction TR_CUSTORD = conB.BeginTransaction())
                        {
                            if (c_del > 0)
                            {
                                Loger.Log("START DELETE_GIUD_cust: " + (DateTime.Now - start));
                                Loger.Log("RECORDS DELETE_cust: " + c_del);
                                using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from cust_ord where \"id\"=@ID", conB))
                                {
                                    cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd.Prepare();
                                    foreach (Orders_row row in _Row_del)
                                    {
                                        cmd.Parameters[0].Value = row.Id;
                                        cmd.ExecuteNonQuery();
                                    }
                                    _Row_del = null;
                                    Loger.Log("END DELETE_GIUD_cust: " + (DateTime.Now - start));
                                }
                            }
                            if (c_mod > 0)
                            {
                                Loger.Log("Start Modify_cust: " + (DateTime.Now - start));
                                Loger.Log("RECORDS Modify cust: " + c_mod);
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.cust_ord " +
                                            "SET koor=@koor, order_no=@order_no, line_no=@line_no, rel_no=@rel_no, line_item_no=@line_item_no, " +
                                                "customer_po_line_no=@customer_po_line_no, dimmension=@dimmension , last_mail_conf=@last_mail_conf, state_conf=@state_conf," +
                                                " line_state=@line_state, cust_order_state=@cust_order_state, country=@country, cust_no=@cust_no, zip_code=@zip_code, addr1=@addr1," +
                                                " prom_date=@prom_date, prom_week=@prom_week, load_id=@load_id, ship_date=@ship_date, part_no=@part_no, descr=@descr," +
                                                " configuration=@configuration, buy_qty_due=@buy_qty_due, desired_qty=@desired_qty, qty_invoiced=@qty_invoiced," +
                                                " qty_shipped=@qty_shipped, qty_assigned=@qty_assigned, dop_connection_db=@dop_connection_db, dop_id=@dop_id," +
                                                " dop_state=@dop_state, data_dop=@data_dop, dop_qty=@dop_qty, dop_made=@dop_made, date_entered=@date_entered, chksum=@chksum," +
                                                " custid=@custid,zest=@zest,seria0=@seria0,data0=@data0,objversion=current_timestamp " +
                                             "where \"id\"=@id;", conB))
                                {
                                    cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_item_no", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@customer_po_line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@dimmension", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@cust_order_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@cust_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@zip_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@addr1", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@buy_qty_due", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@desired_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_invoiced", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_shipped", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_assigned", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@dop_connection_db", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@dop_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@dop_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@dop_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@dop_made", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                    cmd.Parameters.Add("@chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean);
                                    cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd.Prepare();
                                    foreach (Orders_row row in _Row_mod)
                                    {
                                        cmd.Parameters[0].Value = row.Koor;
                                        cmd.Parameters[1].Value = row.Order_no;
                                        cmd.Parameters[2].Value = row.Line_no;
                                        cmd.Parameters[3].Value = row.Rel_no;
                                        cmd.Parameters[4].Value = row.Line_item_no;
                                        cmd.Parameters[5].Value = row.Customer_po_line_no;
                                        cmd.Parameters[6].Value = row.Dimmension;
                                        cmd.Parameters[7].Value = row.Last_mail_conf;
                                        cmd.Parameters[8].Value = row.State_conf;
                                        cmd.Parameters[9].Value = row.Line_state;
                                        cmd.Parameters[10].Value = row.Line_state;
                                        cmd.Parameters[11].Value = row.Country;
                                        cmd.Parameters[12].Value = row.Cust_no;
                                        cmd.Parameters[13].Value = row.Zip_code;
                                        cmd.Parameters[14].Value = row.Addr1;
                                        cmd.Parameters[15].Value = row.Prom_date;
                                        cmd.Parameters[16].Value = row.Prom_week;
                                        cmd.Parameters[17].Value = row.Load_id;
                                        cmd.Parameters[18].Value = row.Ship_date;
                                        cmd.Parameters[19].Value = row.Part_no;
                                        cmd.Parameters[20].Value = row.Descr;
                                        cmd.Parameters[21].Value = row.Configuration;
                                        cmd.Parameters[22].Value = row.Buy_qty_due;
                                        cmd.Parameters[23].Value = row.Desired_qty;
                                        cmd.Parameters[24].Value = row.Qty_invoiced;
                                        cmd.Parameters[25].Value = row.Qty_shipped;
                                        cmd.Parameters[26].Value = row.Qty_assigned;
                                        cmd.Parameters[27].Value = row.Dop_connection_db;
                                        cmd.Parameters[28].Value = row.Dop_id;
                                        cmd.Parameters[28].Value = row.Dop_state;
                                        cmd.Parameters[28].Value = row.Data_dop;
                                        cmd.Parameters[28].Value = row.Dop_qty;
                                        cmd.Parameters[28].Value = row.Dop_made;
                                        cmd.Parameters[28].Value = row.Date_entered;
                                        cmd.Parameters[28].Value = row.Chksum;
                                        cmd.Parameters[28].Value = row.Custid;
                                        cmd.Parameters[28].Value = row.Zest;
                                        cmd.Parameters[28].Value = row.Seria0;
                                        cmd.Parameters[28].Value = row.Data0;
                                        cmd.Parameters[28].Value = row.Id;
                                        cmd.ExecuteNonQuery();
                                    }
                                    _Row_mod = null;
                                    Loger.Log("END Modify_cust: " + (DateTime.Now - start));
                                }
                            }
                            if (c_add > 0)
                            {
                                Loger.Log("START INSERT_cust: " + (DateTime.Now - start));
                                Loger.Log("RECORDS INSERT cust: " + c_add);
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "INSERT INTO public.cust_ord" +
                                            "(koor, order_no, line_no, rel_no, line_item_no,customer_po_line_no,dimmension, last_mail_conf, state_conf, line_state," +
                                                " cust_order_state, country, cust_no, zip_code, addr1, prom_date, prom_week, load_id, ship_date, part_no, descr," +
                                                " configuration, buy_qty_due, desired_qty, qty_invoiced, qty_shipped, qty_assigned, dop_connection_db, dop_id, dop_state," +
                                                " data_dop, dop_qty, dop_made, date_entered, chksum, custid, id,zest,seria0,data0,objversion) " +
                                            "VALUES " +
                                            "(@koor, @order_no, @line_no, @rel_no, @line_item_no,@customer_po_line_no,@dimmension, @last_mail_conf, @state_conf, @line_state," +
                                            " @cust_order_state, @country, @cust_no, @zip_code, @addr1, @prom_date, @prom_week, @load_id, @ship_date, @part_no, @descr," +
                                            " @configuration, @buy_qty_due, @desired_qty, @qty_invoiced, @qty_shipped, @qty_assigned, @dop_connection_db, @dop_id, @dop_state," +
                                            " @data_dop, @dop_qty, @dop_made, @date_entered, @chksum, @custid, @id,@zest,@seria0,@data0,current_timestamp);", conB))
                                {
                                    cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_item_no", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@customer_po_line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@dimmension", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@cust_order_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@cust_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@zip_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@addr1", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@buy_qty_due", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@desired_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_invoiced", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_shipped", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@qty_assigned", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@dop_connection_db", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@dop_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@dop_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@dop_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@dop_made", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                    cmd.Parameters.Add("@chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean);
                                    cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd.Prepare();
                                    foreach (Orders_row row in _Row_add)
                                    {
                                        cmd.Parameters[0].Value = row.Koor;
                                        cmd.Parameters[1].Value = row.Order_no;
                                        cmd.Parameters[2].Value = row.Line_no;
                                        cmd.Parameters[3].Value = row.Rel_no;
                                        cmd.Parameters[4].Value = row.Line_item_no;
                                        cmd.Parameters[5].Value = row.Customer_po_line_no;
                                        cmd.Parameters[6].Value = row.Dimmension;
                                        cmd.Parameters[7].Value = row.Last_mail_conf;
                                        cmd.Parameters[8].Value = row.State_conf;
                                        cmd.Parameters[9].Value = row.Line_state;
                                        cmd.Parameters[10].Value = row.Line_state;
                                        cmd.Parameters[11].Value = row.Country;
                                        cmd.Parameters[12].Value = row.Cust_no;
                                        cmd.Parameters[13].Value = row.Zip_code;
                                        cmd.Parameters[14].Value = row.Addr1;
                                        cmd.Parameters[15].Value = row.Prom_date;
                                        cmd.Parameters[16].Value = row.Prom_week;
                                        cmd.Parameters[17].Value = row.Load_id;
                                        cmd.Parameters[18].Value = row.Ship_date;
                                        cmd.Parameters[19].Value = row.Part_no;
                                        cmd.Parameters[20].Value = row.Descr;
                                        cmd.Parameters[21].Value = row.Configuration;
                                        cmd.Parameters[22].Value = row.Buy_qty_due;
                                        cmd.Parameters[23].Value = row.Desired_qty;
                                        cmd.Parameters[24].Value = row.Qty_invoiced;
                                        cmd.Parameters[25].Value = row.Qty_shipped;
                                        cmd.Parameters[26].Value = row.Qty_assigned;
                                        cmd.Parameters[27].Value = row.Dop_connection_db;
                                        cmd.Parameters[28].Value = row.Dop_id;
                                        cmd.Parameters[28].Value = row.Dop_state;
                                        cmd.Parameters[28].Value = row.Data_dop;
                                        cmd.Parameters[28].Value = row.Dop_qty;
                                        cmd.Parameters[28].Value = row.Dop_made;
                                        cmd.Parameters[28].Value = row.Date_entered;
                                        cmd.Parameters[28].Value = row.Chksum;
                                        cmd.Parameters[28].Value = row.Custid;
                                        cmd.Parameters[28].Value = row.Zest;
                                        cmd.Parameters[28].Value = row.Seria0;
                                        cmd.Parameters[28].Value = row.Data0;
                                        cmd.Parameters[28].Value = row.Id;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                _Row_add = null;
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "update public.cust_ord a " +
                                            "SET zest=case when a.dop_connection_db = 'AUT' then " +
                                                "case when a.line_state='Aktywowana' then " +
                                                    "case when dop_made=0 then " +
                                                        "case when substring(a.part_no,1,1) not in ('5','6','2') then b.zs " +
                                                        "else null	end " +
                                                     "else null end " +
                                                 "else null end else null end " +
                                                 "from " +
                                                    "(select ZEST_ID,CASE WHEN zest>1 THEN zest_id ELSE null END as zs " +
                                                        "from " +
                                                        "(select a.order_no,a.line_no,b.zest,a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week ZEST_ID " +
                                                            "from " +
                                                            "cust_ord a " +
                                                            "left join " +
                                                            "(select id,count(zest) zest " +
                                                                "from " +
                                                                "(select order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week id,part_no zest " +
                                                                    "from cust_ord " +
                                                                    "where line_state!='Zarezerwowana' and dop_connection_db='AUT' and seria0=false " +
                                                                        "and data0 is null group by order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week,part_no ) a " +
                                                               "group by id) b " +
                                                             "on b.id=a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week " +
                                                         "where substring(part_no,1,1) not in ('5','6','2') ) a) b " +
                                                     "where a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week=b.ZEST_ID", conB))
                            {
                                Loger.Log("Cust_ord update zest" + (DateTime.Now - start));
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "Delete from public.late_ord " +
                                "where cust_id in (SELECT a.cust_id " +
                                    "FROM public.late_ord a " +
                                    "left join " +
                                    "public.cust_ord b " +
                                    "on a.cust_id=b.id " +
                                    "where b.id is null or b.line_state='Zarezerwowana' or b.dop_qty=b.dop_made)", conB))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "Delete from public.cust_ord_history " +
                                "where id in (SELECT a.id FROM " +
                                "public.cust_ord_history a " +
                                "left join " +
                                "public.cust_ord b " +
                                "on a.id=b.id " +
                                "where b.id is null)", conB))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='cust_ord'", conB))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            TR_CUSTORD.Commit();
                        }                        
                    }
                    Loger.Log("REaDY cust_ord " + (DateTime.Now - start));
                }
                GC.Collect();
                return 0;
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
                return 1;
            }
        }         
        class Orders_row : IEquatable<Orders_row>,IComparable<Orders_row> 
        {
            public string Koor { get; set; }
            public string Order_no { get; set; }
            public string Line_no { get; set; }
            public string Rel_no { get; set; }
            public int Line_item_no { get; set; }
            public string Customer_po_line_no { get; set; }
            public double? Dimmension { get; set; }
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
            public int? Load_id { get; set; }
            public DateTime? Ship_date { get; set; }
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
            public DateTime? Data_dop { get; set; }
            public double Dop_qty { get; set; }
            public double Dop_made { get; set; }
            public DateTime Date_entered { get; set; }
            public int Chksum { get; set; }
            public int Custid { get; set; }
            public string Zest { get; set; }
            public bool? Seria0 { get; set; }
            public DateTime Data0 { get; set; }
            public Guid Id { get; set; }
            /// <summary>
            /// default Comparer by cust_id
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public int CompareTo(Orders_row other)
            {
                if (other==null)
                {
                    return 1;
                }
                else
                {
                    return this.Custid.CompareTo(other.Custid);
                }
            }
            public bool Equals(Orders_row other)
            {
                if (other == null) return false;
                return (this.Custid.Equals(other.Custid));
            }
        }
    }
}
