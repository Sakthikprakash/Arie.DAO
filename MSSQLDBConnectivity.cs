using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace Arie.DAO
{
    public class MSSQLDBConnectivity
    {
        SqlConnection connection = null;
        SqlCommand command = null;
        SqlDataAdapter sqlAdapter = null;
        SqlParameter sqlParameter = null;

        string connectionString = string.Empty;

        public string message = string.Empty, sPath = string.Empty;

        //FileManagementSystem fileManagementSystem = new FileManagementSystem();
        string startDateTime = string.Empty, endDateTime = string.Empty;

        public MSSQLDBConnectivity(string storeOfflineFileNameWithPath)
        {
            sPath = storeOfflineFileNameWithPath;
        }

        /*      OPENING THE DATABASE CONNECTIVITY IF ALREADY CLOSED     */
        public string openMyDataBase(string dataContent)
        {
            try
            {
                if (connection != null && connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }
                return dataContent;
            }
            catch (SqlException sqlExp)
            {
                message = "Inside openMyDataBase()..\n" + sqlExp.ToString();
                return message;
            }
        }

        /*      CLOSING THE DATABASE CONNECTIVITY IF ALREADY OPENED      */
        public string closeMyDataBase(string dataContent)
        {
            try
            {
                if (connection != null && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                return dataContent;
            }
            catch (SqlException sqlExp)
            {
                message = "Inside closeMyDataBase()..\n" + sqlExp.ToString();
                return message;
            }
        }

        /*      ESTABLISHING THE DATABASE CONNECTION BY PROVINDING THE CONNECTION STRING   */
        public bool dataBaseConnection(string serverName, string dataBaseName, string userName, string password)
        {
            try
            {
                string stringConnection = @"Data Source=" + serverName +
                                            ";Initial Catalog=" + dataBaseName +
                                            ";User ID=" + userName +
                                            ";Password=" + password +
                                            ";connection timeout=900;";
                connection = new SqlConnection(stringConnection);
                connection.Open();
                return true;
            }
            catch (SqlException sqlExp)
            {
                message = "Inside dataBaseConnection()..\n" + sqlExp.Message;
                return false;
            }
        }

        /*      ESTABLISHING THE DATABASE CONNECTION BY PROVINDING THE CONNECTION STRING   */
        public bool dataBaseConnection(string connString)
        {
            try
            {
                if (!string.IsNullOrEmpty(connString))
                {
                    connection = new SqlConnection(connString);
                    connection.Open();
                    return true;
                }
                else
                {
                    return false;
                }

            }
            catch (SqlException sqlExp)
            {
                message = "Inside dataBaseConnection()..\n" + sqlExp.Message;
                return false;
            }
        }

        /*      PROVIDING THE NORMAL STRING QUERY TO GET THE RESULT IN DATATABLE FORMAT       */
        public bool dataBaseCommand(string queryStatement, bool isQuery, ref DataTable dataTable, ref int updateCount, string folderPath, string fileName)
        {
            string stringBuilding = string.Empty;
            try
            {
                startDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += openMyDataBase("OPENED BY [" + startDateTime + "] ..\n");
                command = new SqlCommand(queryStatement, connection);
                if (isQuery)
                {
                    dataTable = new DataTable();
                    sqlAdapter = new SqlDataAdapter(command);
                    sqlAdapter.Fill(dataTable);
                }
                else
                {
                    updateCount = command.ExecuteNonQuery();
                }
                dataTable.AcceptChanges();
                stringBuilding += "INSIDE TRY..\n";
                return true;
            }
            catch (SqlException sqlExp)
            {
                message = sqlExp.ToString();
                stringBuilding += "INSIDE EXCEPTION..[" + message + "]..\n";
                return false;
            }
            finally
            {
                endDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += " - " + Convert.ToDateTime(endDateTime).Subtract(Convert.ToDateTime(startDateTime)) + "\n";
                stringBuilding += closeMyDataBase("FINALLY CLOSED..\n");
                //fileManagementSystem.checkForFolderExistsOrNot(folderPath, fileName, stringBuilding);
            }
        }

        /*      PROVIDING THE PROCEDURE NAME WITH LOCK TO GET THE RESULT IN DATATABLE FORMAT       */
        public bool callStoredProcedure(string procedureName, bool isQuery, Object[,] variables, ref DataSet dataSet, ref int updateCount
            , string folderPath, string fileName, ref string errorMessage)
        {
            SqlTransaction transaction = null;
            string stringBuilding = string.Empty;
            //message = string.Empty;
            int variable_Count = 0;
            try
            {
                startDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += openMyDataBase("OPENED BY [" + startDateTime + "] ..\n");
                transaction = connection.BeginTransaction(IsolationLevel.Serializable);
                command = new SqlCommand(procedureName, connection, transaction);
                command.CommandType = CommandType.StoredProcedure;
                if (variables != null)
                {
                    variable_Count = variables.Length / 2;
                    for (int i = 0; i < variable_Count; i++)
                    {
                        stringBuilding += "@" + variables[i, 0] + " = " + variables[i, 1] + "\n";
                        sqlParameter = new SqlParameter("@" + variables[i, 0], variables[i, 1]);
                        command.Parameters.Add(sqlParameter);
                    }
                }
                if (isQuery)
                {
                    dataSet = new DataSet();
                    sqlAdapter = new SqlDataAdapter(command);
                    sqlAdapter.Fill(dataSet);
                }
                else
                {
                    updateCount = command.ExecuteNonQuery();
                }
                transaction.Commit();
                dataSet.AcceptChanges();
                stringBuilding += "INSIDE TRY..\n";
                return true;
            }
            catch (Exception exception)
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                errorMessage = exception.ToString();
                stringBuilding += "INSIDE EXCEPTION..[" + errorMessage + "]..\n";
                return false;
            }
            finally
            {
                endDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += procedureName + " - " + Convert.ToDateTime(endDateTime).Subtract(Convert.ToDateTime(startDateTime)) + "\n";
                stringBuilding += closeMyDataBase("FINALLY CLOSED..\n");
                //fileManagementSystem.checkForFolderExistsOrNot(folderPath, fileName, stringBuilding);
            }
        }

        /*      PROVIDING THE STRING QUERY WITH LOCK TO GET THE RESULT IN DATATABLE FORMAT       */
        public bool dataBaseLockCommand(string queryStatement, bool isQuery, ref DataSet dataSet, ref int updateCount, string folderPath, string fileName)
        {
            SqlTransaction transaction = null;
            string stringBuilding = string.Empty;
            try
            {
                startDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += openMyDataBase("OPENED BY [" + startDateTime + "] ..\n");
                transaction = connection.BeginTransaction(IsolationLevel.Serializable);
                command = new SqlCommand(queryStatement, connection, transaction);
                command.CommandTimeout = 0;
                //command.CommandText = queryStatement;
                //command.Connection = connection;                
                //command.Transaction = transaction;
                if (isQuery)
                {
                    dataSet = new DataSet();
                    sqlAdapter = new SqlDataAdapter(command);

                    sqlAdapter.Fill(dataSet);
                }
                else
                {
                    updateCount = command.ExecuteNonQuery();
                }
                transaction.Commit();
                dataSet.AcceptChanges();
                stringBuilding += "INSIDE TRY..\n";
                return true;
            }
            catch (SqlException sqlExp)
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                message = sqlExp.ToString();
                stringBuilding += "INSIDE EXCEPTION..[" + message + "]..\n";
                return false;
            }
            finally
            {
                endDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffff");
                stringBuilding += " - " + Convert.ToDateTime(endDateTime).Subtract(Convert.ToDateTime(startDateTime)) + "\n";
                stringBuilding += closeMyDataBase("FINALLY CLOSED..\n");
                //fileManagementSystem.checkForFolderExistsOrNot(folderPath, fileName, stringBuilding);
            }
        }

        /*      DISPLAY THE INFORMATION       */
        public string showDBInformation()
        {
            return message;
        }

        /*      TO GET THE DATABASE LIST FROM THE SERVER       */
        public ArrayList getListOfDataBases()
        {
            try
            {
                ArrayList dataBaseArrayList = new ArrayList();
                DataTable dataTable = new DataTable();
                int count = 0;
                string getDataBaseListQuery = "SELECT name FROM master..sysdatabases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');";

                if (dataBaseCommand(getDataBaseListQuery, true, ref dataTable, ref count, null, null))
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        dataBaseArrayList.Add(dataTable.Rows[i]["name"].ToString());
                    }
                    getDataBaseListQuery = string.Empty;
                    return dataBaseArrayList;
                }
                else
                {
                    return null;
                }
            }
            catch (SqlException sqlExp)
            {
                message = "Inside getListOfDataBases()..\n" + sqlExp.ToString();
                return null;
            }
        }

        /*      TO GET THE TABLE NAME LIST FOR THE CHOOSEN DATABASE       */
        public ArrayList getListOfTables(string dataBaseName)
        {
            try
            {
                ArrayList tableArrayList = new ArrayList();
                DataTable dataTable = new DataTable();
                int count = 0;
                string getTableListQuery = "SELECT TABLE_NAME FROM " + dataBaseName + ".INFORMATION_SCHEMA.TABLES";

                if (dataBaseCommand(getTableListQuery, true, ref dataTable, ref count, null, null))
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        tableArrayList.Add(dataTable.Rows[i]["TABLE_NAME"].ToString());
                    }
                    getTableListQuery = string.Empty;
                    return tableArrayList;
                }
                else
                {
                    return null;
                }
            }
            catch (SqlException sqlExp)
            {
                message = "Inside getListOfTables()..\n" + sqlExp.ToString();
                return null;
            }
        }

        /*      TO GET THE FIELD NAME LIST FOR THE CHOOSEN TABLE NAME       */
        public ArrayList getFieldNameListForTable(string dataBaseName, string tableName)
        {
            try
            {
                openMyDataBase("OPEN CONNECTION..\n");
                {
                    ArrayList fieldNameArrayList = new ArrayList();
                    DataTable dataTable = new DataTable();
                    int count = 0;
                    string getFieldNameListQuery = "SELECT COLUMN_NAME FROM " + dataBaseName + ".INFORMATION_SCHEMA.COLUMNS" +
                                                    " WHERE TABLE_NAME = '" + tableName + "'";

                    if (dataBaseCommand(getFieldNameListQuery, true, ref dataTable, ref count, null, null))
                    {
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            fieldNameArrayList.Add(dataTable.Rows[i]["COLUMN_NAME"].ToString());
                        }
                        getFieldNameListQuery = string.Empty;
                        return fieldNameArrayList;
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            catch (SqlException sqlExp)
            {
                message = "Inside getFieldNameListForTable()..\n" + sqlExp.ToString();
                return null;
            }
        }

        /*      TO GET THE FIELD TYPE FOR THE GIVEN FIELD NAME      */
        public string getFieldTypeForTables(string dataBaseName, string tableName, string fieldName)
        {
            try
            {
                DataTable dataTable = new DataTable();
                int count = 0;
                string getFieldTypeListQuery = "SELECT DATA_TYPE FROM " + dataBaseName + ".INFORMATION_SCHEMA.COLUMNS" +
                                                " WHERE TABLE_NAME = '" + tableName + "'" +
                                                " AND COLUMN_NAME = '" + fieldName + "'";

                if (dataBaseCommand(getFieldTypeListQuery, true, ref dataTable, ref count, null, null))
                {
                    if (dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0]["DATA_TYPE"].ToString();
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }
            catch (SqlException sqlExp)
            {
                message = "Inside getFieldTypeForTables()..\n" + sqlExp.ToString();
                return null;
            }
        }

        /*      TO GET THE DEFAULT VALUE OF FIELD TYPE FOR THE GIVEN FIELD NAME      */
        public bool getDefaultValueOfFieldTypeForTables(string dataBaseName, string tableName, string fieldName)
        {
            try
            {
                DataTable dataTable = new DataTable();
                int count = 0;
                string getFieldTypeListQuery = "SELECT IS_NULLABLE FROM " + dataBaseName + ".INFORMATION_SCHEMA.COLUMNS" +
                                                        " WHERE TABLE_NAME = '" + tableName + "'" +
                                                        " AND COLUMN_NAME = '" + fieldName + "'";

                if (dataBaseCommand(getFieldTypeListQuery, true, ref dataTable, ref count, null, null))
                {
                    if (dataTable.Rows.Count > 0)
                    {
                        return dataTable.Rows[0]["IS_NULLABLE"].ToString().Equals("Yes", StringComparison.OrdinalIgnoreCase) ? true : false;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            catch (SqlException sqlExp)
            {
                message = "Inside getDefaultValueOfFieldTypeForTables()..\n" + sqlExp.ToString();
                return false;
            }
        }
    }
}