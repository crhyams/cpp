using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Net.Mail;
using System.Net;
using System.Text.RegularExpressions;
using System.Security.Cryptography;

namespace CPP_LoadDB
{
    class CPP_ControlClass
    {
        static void Main(string[] args)
        {
            string[] PendingFiles = null;

            // Instantiate the Class
            CPP_FileImport_Class LoadLog = new CPP_FileImport_Class();
            CPP_billHead billhead = new CPP_billHead();

            // Grab files in the staging folder
            try
            {
                PendingFiles = Directory.GetFiles(Properties.Settings.Default.inputFolder);
            }
            catch (Exception ex)
            {
                StringBuilder emailBody = new StringBuilder();
                string emailConnectionString = Properties.Settings.Default.emailConnectionString;

                // Send an email when the file getting fails
                MailMessage message = new MailMessage();

                message.To.Add(Properties.Settings.Default.ErrorEmailDist);
                message.From = new MailAddress("automation@sourcelink.com", "JetVision Log Importer");
                message.Subject = "###ERROR### CPP Processing ###ERROR###";
                emailBody.AppendLine("There was an error getting the list of JetVision Log files to import.");
                emailBody.AppendLine();
                emailBody.AppendLine("See Below for Error details:");
                emailBody.AppendLine(String.Format("{0}", ex.Message));
                emailBody.AppendLine();
                emailBody.AppendLine("Stack Trace:");
                emailBody.AppendLine(String.Format("{0}", ex.StackTrace));
                emailBody.AppendLine();
                emailBody.AppendLine("Thanks,");
                emailBody.AppendLine("SourceLink Carolina");

                message.Body = emailBody.ToString();

              //  SmtpClient smtp = new SmtpClient("mail.carolina.sourcelink.com");
                SmtpClient smtp = new SmtpClient(emailConnectionString);
                smtp.Credentials = new NetworkCredential("automation", "automating");
                smtp.Send(message);

                message.Dispose();
            }

            // Process all files found
            foreach (string _strFileToProcess in PendingFiles)
            {
                LoadLog.Import(_strFileToProcess);
            }
        }

    }

    class CPP_FileImport_Class
    {
        private string _strLogFile;
        private string _strLogFolder = Properties.Settings.Default.LogFolder;
        private string _strConnectionString = Properties.Settings.Default.ConnectionString;
        private string _strArchiveFolder = Properties.Settings.Default.ArchiveFolder;
      //  private string _strStagingFolder = Properties.Settings.Default.StagingFolder;
      //  private string _strStagedFile;
        private long _intLinesInFile;
        private int _intRecordsImported;
        private int _intAlreadyProcessed;
        private int _intPendingRecsVerified;
        private int _intNonMatchCount;
        private int _intRetVal;
        private string _strRetMsg;
        private bool _bolImportSQLErrorsEncountered;


        public void Import(string _strFileToImport)
        {
            bool _bolContinue = false;
            _intRecordsImported = 0;
            _bolImportSQLErrorsEncountered = false;
            bool _bolDuplicateFile = false;
            string _strMD5 = "";

            try
            {
                // Set the Log filee
                _strLogFile = Path.Combine(_strLogFolder, DateTime.Now.ToString("yyyyMMdd", System.Globalization.CultureInfo.GetCultureInfo("en-US")) + ".log");

                WriteToLog(Environment.NewLine + "Processing file {" + Path.GetFileName(_strFileToImport) + "}");
                _bolDuplicateFile = VerifyChecksum(_strFileToImport, out _strMD5);

                if (_bolDuplicateFile)
                {
                    WriteToLog("... Checksum matched previous file {" + _strMD5 + "}");
                    WriteToLog("... Deleting file!");
                    File.Delete(_strFileToImport);

                    return;
                }

                // Get count of lines in .csv file
                _intLinesInFile = CountLinesInFile(_strFileToImport, out _bolContinue);
                _intLinesInFile = readRecordKeys(_strFileToImport, out _bolContinue);
                // If invalid column count, exit
                if (!_bolContinue)
                {
                    return;
                }

                // Move the file to staging
                //_strStagedFile = Path.Combine(_strStagingFolder, Path.GetFileName(_strFileToImport));
                //File.Copy(_strFileToImport, _strStagedFile, true);
                //File.Delete(_strFileToImport);

                // Establish the SQL Connection
                using (SqlConnection SQLconn = new SqlConnection(_strConnectionString))
                {
                    SQLconn.Open();

                    // Import file
                  //  BulkInsertFile(_strStagedFile, SQLconn, out _intRecordsImported, out _bolContinue);

                    // Was Job indexed?  If not, log and exit
              //      if (!_bolContinue)
               //     {
               //         WriteToLog("...No Job/Package/Drops Indexed for the provided JetVision file.");
               //         return;
               //     }

                    // Compare line count with record count
                 //   if (_intLinesInFile != _intRecordsImported)
                 //   {
                 //       WriteToLog("...Line count in file {" + _intLinesInFile.ToString() + "} does not match number of records imported {" + _intRecordsImported.ToString() + "}");
                 //       EmailFailureLog(_strStagedFile, "Line count {" + _intLinesInFile.ToString() + "} in file {" + Path.GetFileName(_strStagedFile) + "} does not match number of records imported {"
                 //           + _intRecordsImported.ToString() + "}!");
                 //       return;
                 //   }
                 //   else
                 //   {
                 ///       WriteToLog("...Line count in file {" + _intLinesInFile.ToString() + "} matches number of records imported {" + _intRecordsImported.ToString() + "}");
                 //       WriteToLog("...ALL RECORDS SUCCESSFULLY IMPORTED!" + Environment.NewLine);
                 //   }

                //    SendAlertEmail(_strStagedFile, SQLconn);
                }

            }
            catch (Exception ex)
            {
                WriteToLog("EXCEPTION ENCOUNTERED!" + Environment.NewLine + Environment.NewLine + ex.Message);
          //      EmailFailureLog(_strStagedFile, "Exception encountered");
            }
            finally
            {
                if (!_bolDuplicateFile)
                {
            //        MoveInputFileToArchive(_strStagedFile);
                }

                WriteToLog("***************************************************************************************************************************");
            }
        }

        private bool VerifyChecksum(string _strFileToImport, out string _strMD5)
        {
            int _intChecksumMatched = 0;

            try
            {
                // Calculate MD5
                using (var md5 = MD5.Create())
                {
                    using (var stream = File.OpenRead(_strFileToImport))
                    {
                        _strMD5 = BitConverter.ToString(md5.ComputeHash(stream)).Replace("-", "").ToLower();
                    }
                }

                // Does the Checksum match previous file?
                using (SqlConnection SQLconn = new SqlConnection(_strConnectionString))
                {
                    SQLconn.Open();

                    SqlCommand cmdCheckForDuplicateFile = new SqlCommand("If exists (Select 1 from tblFileReceived Where FileChecksum = @Checksum) Select 1 Else Select 0", SQLconn);
                    cmdCheckForDuplicateFile.Parameters.Add(new SqlParameter("Checksum", _strMD5));
                    _intChecksumMatched = (int)cmdCheckForDuplicateFile.ExecuteScalar();

                    if (_intChecksumMatched == 1)
                    {
                        return true;
                    }
                    else
                    {
                        // Log the new Checksum
                        SqlCommand cmdAddChecksum = new SqlCommand("Insert into tblFileReceived (FileChecksum, FileUploaded) Select @Checksum, @FileUploaded", SQLconn);
                        cmdAddChecksum.Parameters.Add(new SqlParameter("Checksum", _strMD5));
                        cmdAddChecksum.Parameters.Add(new SqlParameter("FileUploaded", Path.GetFileName(_strFileToImport)));
                        cmdAddChecksum.ExecuteNonQuery();
                        return false;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        private void SendAlertEmail(string _strStagedFile, SqlConnection SQLconn)
        {
            string _strEmailBody;

            try
            {
                // Were any already processed records or non matches found?
                if (_intAlreadyProcessed > 0 || _intNonMatchCount > 0 || _bolImportSQLErrorsEncountered)
                {
                    // Create new message object
                    MailMessage message = new MailMessage();

                    // Add to:
                    message.To.Add(Properties.Settings.Default.ErrorEmailDist);

                    // Add from:
                    message.From = new MailAddress("automation@sourcelink.com");

                    // Subject
                    if (_bolImportSQLErrorsEncountered)
                    {
                        message.Subject = "JetVision Log - SQL Import errors encountered!  See attached Log";
                    }
                    else
                    {
                        message.Subject = "JetVision Log - Processed Indexes and/or Non Matching TwoDs encountered!";
                    }

                    // Message body
                    _strEmailBody = "Run Date: " + DateTime.Now.ToString("f") + "  " + Environment.NewLine
                        + "File: " + Path.GetFileName(_strStagedFile) + Environment.NewLine
                        + "\r\n"; // for some reason, it was not going to the next line when included above.

                    _strEmailBody += "Log records imported: " + _intRecordsImported.ToString() + " \r\n";
                    _strEmailBody += "Pending pieces verified: " + _intPendingRecsVerified.ToString() + " \r\n";
                    _strEmailBody += "Pre-verified piece matches: " + _intAlreadyProcessed.ToString() + "\r\n";
                    _strEmailBody += "Non matching TwoD's: " + _intNonMatchCount.ToString() + "\r\n\r\n";

                    // Non Matches
                    if (_intNonMatchCount > 0)
                    {
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "--- NON MATCHED TWO D's --- \n";
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "**********************************************" + "\n";

                        SqlCommand cmdNonMatch = new SqlCommand("Select JetVisionLogKey, TwoD, JobNo, PackageParsed, DropParsed from ##Reprints_JetVisionNonMatches", SQLconn);
                        SqlDataReader readerNonMatch = cmdNonMatch.ExecuteReader();

                        while (readerNonMatch.Read())
                        {
                            _strEmailBody += "JetVisionLogKey: " + readerNonMatch["JetVisionLogKey"].ToString() + "\r\n";
                            _strEmailBody += "Job/Package/Drop: " + readerNonMatch["JobNo"].ToString().Trim() + "p" + readerNonMatch["PackageParsed"].ToString().Trim() +
                                "d" + readerNonMatch["DropParsed"].ToString().Trim() + "\r\n";
                            _strEmailBody += "TwoD: " + readerNonMatch["TwoD"] + "\r\n";

                            // Add a blank line between instances reported
                            _strEmailBody += "**********************************************" + "\r\n";
                        }

                        readerNonMatch.Close();
                    }

                    // Add a blank line between sections
                    _strEmailBody += "\r\n";

                    // Already Processed
                    if (_intAlreadyProcessed > 0)
                    {
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "--- PRE-VERIFIED PIECE MATCHES --- \n";
                        _strEmailBody += "**********************************************" + "\n";
                        _strEmailBody += "**********************************************" + "\n";

                        SqlCommand cmdPreVerified = new SqlCommand(
                            "Select JetVisionLogKey, IndexKey, Job, Package, DropNo, Seq, TwoD, VerifiedCount, DateProcessed, OriginalSourceFile, ReprintCount From ##Reprints_JetVisionMatchingProcessedIndex", SQLconn);
                        SqlDataReader readerPreVerified = cmdPreVerified.ExecuteReader();

                        while (readerPreVerified.Read())
                        {
                            _strEmailBody += "JetVisionLogKey: " + readerPreVerified["JetVisionLogKey"].ToString() + "\r\n";
                            _strEmailBody += "IndexKey: " + readerPreVerified["IndexKey"].ToString() + "\r\n";
                            _strEmailBody += "Job/Package/Drop: " + readerPreVerified["Job"].ToString().Trim() + "p" + readerPreVerified["Package"].ToString().Trim() +
                                "d" + readerPreVerified["DropNo"].ToString().Trim() + "\r\n";
                            _strEmailBody += "Seq: " + readerPreVerified["Seq"].ToString() + "\r\n";
                            _strEmailBody += "TwoD: " + readerPreVerified["TwoD"] + "\r\n";
                            _strEmailBody += "Verified Count: " + readerPreVerified["VerifiedCount"].ToString() + "\r\n";
                            _strEmailBody += "Reprint Count: " + readerPreVerified["ReprintCount"].ToString() + "\r\n";
                            _strEmailBody += "Original Source File: " + readerPreVerified["OriginalSourceFile"].ToString() + "\t" + "\r\n";
                            _strEmailBody += "Date of original verification: " + readerPreVerified["DateProcessed"].ToString() + "\r\n";

                            // Add a blank line between instances reported
                            _strEmailBody += "**********************************************" + "\r\n";
                        }

                        readerPreVerified.Close();
                    }

                    // Add a blank line between sections
                    _strEmailBody += "\r\n";

                    // Add Disclaimer
                    _strEmailBody += Environment.NewLine + Environment.NewLine + "This is an automated message.";

                    // Scrub Body String for proper formatting (prevent missing line breaks) in Outlook
                    //Regex.Replace(_strEmailBody, @"(?<!\t)((?<!\r)(?=\n)|(?=\r\n))", "\t", RegexOptions.Multiline);

                    message.Body = _strEmailBody;

                    SmtpClient smtp = new SmtpClient("mail.carolina.sourcelink.com");
                    smtp.Credentials = new NetworkCredential("automation", "automating");
                    smtp.Send(message);

                    // Dispose email
                    message.Dispose();
                }
            }
            catch
            {
                throw;
            }
        }

        public void MoveInputFileToArchive(string _strFile)
        {
            try
            {
                string _strArchiveFile = Path.Combine(_strArchiveFolder, Path.GetFileName(_strFile));

                File.Copy(_strFile, _strArchiveFile, true);

                File.Delete(_strFile);
            }
            catch
            {
                throw;
            }
        }

        private void BulkInsertFile(string _strFileToImport, SqlConnection SQLconn, out int _intRecordsImported, out bool _bolContinue)
        {
            int _intImportedRecCount;
            _intRecordsImported = 0;

            try
            {
                SqlCommand cmdBulkInsert = new SqlCommand("spBulkInsertJetVisionLog", SQLconn);
                cmdBulkInsert.CommandType = CommandType.StoredProcedure;
                cmdBulkInsert.CommandTimeout = 600;

                SqlParameter param1 = new SqlParameter("@i_SourceFile", SqlDbType.VarChar);
                param1.Size = 255;
                param1.Direction = ParameterDirection.Input;
                param1.Value = _strFileToImport;
                cmdBulkInsert.Parameters.Add(param1);

                SqlParameter param2 = new SqlParameter("@o_RecordImportCount", SqlDbType.Int);
                param2.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param2);

                SqlParameter param3 = new SqlParameter("@o_AlreadyProcessedCount", SqlDbType.Int);
                param3.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param3);

                SqlParameter param4 = new SqlParameter("@o_PendingVerifiedCount", SqlDbType.Int);
                param4.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param4);

                SqlParameter param5 = new SqlParameter("@o_NoMatchesCount", SqlDbType.Int);
                param5.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param5);

                SqlParameter param6 = new SqlParameter("@o_JobNotIndexed", SqlDbType.Int);
                param6.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param6);

                SqlParameter param7 = new SqlParameter("@o_RetVal", SqlDbType.Int);
                param7.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param7);

                SqlParameter param8 = new SqlParameter("@o_RetMsg", SqlDbType.VarChar);
                param8.Size = 5000;
                param8.Direction = ParameterDirection.Output;
                cmdBulkInsert.Parameters.Add(param8);

                WriteToLog("...Executing spBulkInsertJetVisionLog");
                cmdBulkInsert.ExecuteNonQuery();

                // Capture output
                _intImportedRecCount = (int)cmdBulkInsert.Parameters["@o_RecordImportCount"].Value;
                _intAlreadyProcessed = (int)cmdBulkInsert.Parameters["@o_AlreadyProcessedCount"].Value;
                _intPendingRecsVerified = (int)cmdBulkInsert.Parameters["@o_PendingVerifiedCount"].Value;
                _intNonMatchCount = (int)cmdBulkInsert.Parameters["@o_NoMatchesCount"].Value;
                _intRetVal = (int)cmdBulkInsert.Parameters["@o_RetVal"].Value;
                _strRetMsg = (string)cmdBulkInsert.Parameters["@o_RetMsg"].Value;

                _intRecordsImported = _intImportedRecCount;

                // Errors in SP
                if (!string.IsNullOrEmpty(_strRetMsg))
                {
                    WriteToLog("...ERRORS ENCOUNTERED EXECUTING PROC!" + Environment.NewLine + Environment.NewLine + _strRetMsg);
                    _bolImportSQLErrorsEncountered = true;
                }

                // If Job not indexed, do not process file further
                if ((int)cmdBulkInsert.Parameters["@o_JobNotIndexed"].Value == 1)
                {
                    _bolContinue = false;
                }
                else
                {
                    // Write return values to log
                    WriteToLog("......ImportedRecCount: " + _intImportedRecCount.ToString() + Environment.NewLine + "......AlreadyProcessedCount: " + _intAlreadyProcessed.ToString()
                        + Environment.NewLine + "......PendingRecsVerified: " + _intPendingRecsVerified.ToString()
                        + Environment.NewLine + "......NonMatchCount: " + _intNonMatchCount.ToString()
                        + Environment.NewLine + "......RetVal: " + _intRetVal.ToString()
                        + Environment.NewLine + "......RetMsg: " + _strRetMsg);

                    _bolContinue = true;
                }
            }
            catch
            {
                throw;
            }
        }

        private void WriteToLog(string _strMessage)
        {
            try
            {
                TextWriter tw = new StreamWriter(_strLogFile, true);
                tw.WriteLine(_strMessage);
                tw.Close();
            }
            catch
            {
                throw;
            }
        }

        public void EmailFailureLog(string _strFileToImport, string _strErrorMsg)
        {
            string _strEmailBody;

            try
            {
                // Create new message object
                MailMessage message = new MailMessage();

                // Add to:
                message.To.Add(Properties.Settings.Default.ErrorEmailDist);

                // Add from:
                message.From = new MailAddress("automation@sourcelink.com");

                // Subject
                message.Subject = "JetVision Log Import Failure!";

                // Message body
                _strEmailBody = "Run Date: " + DateTime.Now.ToString("f") + "  ";
                _strEmailBody += "\r\n"; // for some reason, it was not going to the next line when included above.
                _strEmailBody += "File: " + Path.GetFileName(_strFileToImport) + Environment.NewLine;

                _strEmailBody += Environment.NewLine + "Error Message: " + Environment.NewLine + _strErrorMsg;


                _strEmailBody += Environment.NewLine;
                _strEmailBody += Environment.NewLine;
                _strEmailBody += "This is an automated message.";
                message.Body = _strEmailBody;

                // Attachment
                message.Attachments.Add(new Attachment(_strLogFile));

                SmtpClient smtp = new SmtpClient("mail.carolina.sourcelink.com");
                smtp.Credentials = new NetworkCredential("automation", "automating");
                smtp.Send(message);

                // Dispose email
                message.Dispose();

            }
            catch
            {
                throw;
            }
        }

        private long CountLinesInFile(string f, out bool _bolContinue)
        {
            long count = 0;
            int _intRowNumber = 1;
            string[] _strSplitLine;
            int _intNumberOfColumns = 0;



            



            using (StreamReader r = new StreamReader(f))
            {
                string line;
                while ((line = r.ReadLine()) != null)
                {

               

                  /*
                    if (_intRowNumber <= 50) // Check the number of clumns in the first 50 rows
                    {
                        _strSplitLine = line.Split(',');
                        _intNumberOfColumns = _strSplitLine.Count();

                        // 13 columns?
                        if (_intNumberOfColumns != 10)
                        {
                            WriteToLog("...Column count {" + _intNumberOfColumns.ToString() + "} in row {" + _intRowNumber.ToString() + "} is invalid.  Required count is 10.  File not imported!");
                            EmailFailureLog(Path.GetFileName(f), "Column count {" + _intNumberOfColumns.ToString() + "} in row {" + _intRowNumber.ToString() + "} in file {"
                                + Path.GetFileName(f) + "}  is invalid.  Required count is 10.  File not imported!");
                            _bolContinue = false;
                            return 0;
                        }

                        _intRowNumber += 1;
                    }
                   */ 
                    count++;
                }
            }
            _bolContinue = true;
            return count;
        }
        private long readRecordKeys(string f, out bool _bolContinue)
        {
            long count = 0;
            int _intRowNumber = 1;
            string[] _strSplitLine;
            int _intNumberOfColumns = 0;
            
            using (StreamReader r = new StreamReader(f))
            {
                using (SqlConnection SQLconn = new SqlConnection(_strConnectionString))
                {
                    SQLconn.Open();

                    string line;
                    while ((line = r.ReadLine()) != null)
                    {

                        switch (line.Substring(0, 8).ToUpper())
                        {
                            case "BILLHEAD":

                                /*
                                SqlCommand cmdAddBillHead = new SqlCommand("Insert into billhead (Record_Type_Label, Bill_Number, Bill_Type, Number_Copies, Bill_Print_Date, Bill_Print_Time, Billing_Date, Address_Name, Address_Line1,Address_Line2, Address_Line3, Address_Line4, Address_Line5, City, State, Zip_Code, Nation, Account_Number, Bill_Cycle_Code, Unused, Acct_Check_Digit, Unused1, Amt_Check_Digit, Unused2, Delinquency_Flag, Suppress_Paper_Bill, Insert_Select_1, Insert_Select_2, Message_File_ID, Unused3, Delinquency_Cut_Off_Date) Select @Record_Type_Label, @Bill_Number, @Bill_Type, @Number_Copies, @Bill_Print_Date, @Bill_Print_Time, @Billing_Date, @Address_Name, @Address_Line1, @Address_Line2, @Address_Line3, @Address_Line4, @Address_Line5, @City, @State, @Zip_Code, @Nation, @Account_Number, @Bill_Cycle_Code, @Unused, @Acct_Check_Digit, @Unused1, @Amt_Check_Digit, @Unused2, @Delinquency_Flag, @Suppress_Paper_Bill, @Insert_Select_1, @Insert_Select_2, @Message_File_ID, @Unused3, @Delinquency_Cut_Off_Date", SQLconn);
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Record_Type_Label", line.Substring(0, 8)));     // 1 
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Bill_Number", line.Substring(8, 6)));           // 2
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Bill_Type", line.Substring(14, 2)));            // 3
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Number_Copies", line.Substring(16, 2)));        // 4
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Bill_Print_Date", line.Substring(18, 11)));     // 5
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Bill_Print_Time", line.Substring(29, 8)));      // 6
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Billing_Date", line.Substring(37, 11)));        // 7
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Name", line.Substring(48, 40)));        // 8
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Line1", line.Substring(88, 30)));       // 9
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Line2", line.Substring(118, 50)));      // 10
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Line3", line.Substring(168, 30)));      // 11
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Line4", line.Substring(198, 30)));      // 12
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Address_Line5", line.Substring(228, 13)));      // 13
                                cmdAddBillHead.Parameters.Add(new SqlParameter("City", line.Substring(241, 20)));               // 14
                                cmdAddBillHead.Parameters.Add(new SqlParameter("State", line.Substring(261, 3)));               // 15
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Zip_Code", line.Substring(264, 11)));           // 16
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Nation", line.Substring(275, 28)));             // 17
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Account_Number", line.Substring(303, 14)));     // 18
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Bill_Cycle_Code", line.Substring(317, 7)));     // 19
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Unused", line.Substring(324, 6)));              // 20
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Acct_Check_Digit", line.Substring(330, 1)));    // 21
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Unused1", line.Substring(331, 1)));             // 22
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Amt_Check_Digit", line.Substring(332, 1)));     // 23
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Unused2", line.Substring(333,  4)));             // 24
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Delinquency_Flag", line.Substring(337, 1)));    // 25
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Suppress_Paper_Bill", line.Substring(338, 1))); // 26
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Insert_Select_1", line.Substring(339, 1)));     // 27
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Insert_Select_2", line.Substring(340, 1)));     // 28
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Message_File_ID", line.Substring(341, 6)));     // 29
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Unused3", line.Substring(347, 14)));            // 30
                                cmdAddBillHead.Parameters.Add(new SqlParameter("Delinquency_Cut_Off_Date", line.Substring(361, 10)));  // 31
                                cmdAddBillHead.ExecuteNonQuery();
                                */
                                break;
                            case "SERVADDR":

                                /*
                                SqlCommand addSERVADDR = new SqlCommand("Insert into SERVADDR (Record_Type_Label, Bill_Number, Service_Address) Select @Record_Type_Label, @Bill_Number, @Service_Address", SQLconn);
                                addSERVADDR.Parameters.Add(new SqlParameter("Record_Type_Label", line.Substring(0, 8)));     // 1 
                                addSERVADDR.Parameters.Add(new SqlParameter("Bill_Number", line.Substring(8, 6)));           // 2
                                addSERVADDR.Parameters.Add(new SqlParameter("Service_Address", line.Substring(14, 63)));            // 3
                                addSERVADDR.ExecuteNonQuery();
                                */
                                break;
                            case "UTILADDR":
                                
                                /*
                                 * SqlCommand addUTILADDR = new SqlCommand("Insert into UTILADDR (Record_Type_Label, Bill_Number, Conditional_Utility_Name, Conditional_Utility_Address_Line1, Conditional_Utility_Address_Line2, Conditional_Utility_Address_Line3, Conditional_Utility_Address_Line4, Fileid) Select @Record_Type_Label, @Bill_Number, @Conditional_Utility_Name, @Conditional_Utility_Address_Line1, @Conditional_Utility_Address_Line2, @Conditional_Utility_Address_Line3, @Conditional_Utility_Address_Line4, @Fileid", SQLconn);
                                addUTILADDR.Parameters.Add(new SqlParameter("Record_Type_Label", line.Substring(0, 8)));     // 1 
                                addUTILADDR.Parameters.Add(new SqlParameter("Bill_Number", line.Substring(8, 6)));           // 2
                                addUTILADDR.Parameters.Add(new SqlParameter("Service_Address", line.Substring(14, 63)));            // 3
                                addUTILADDR.ExecuteNonQuery();
                                Record_Type_Label, Bill_Number, Conditional_Utility_Name, Conditional_Utility_Address_Line1, Conditional_Utility_Address_Line2, Conditional_Utility_Address_Line3, Conditional_Utility_Address_Line4, Fileid, RecordKey
                                */

                                break;
                            case "OBILLAGY":
                                break;
                            case "WEATINFO":
                                break;
                            case "PAYMENTS":
                                break;
                            case "BUDGINFO":
                                break;
                            case "BILLDETL":
                                break;
                            case "CHRGSUMM":
                                break;
                            case "HISTINFO":
                                break;
                            case "MSSGLINE":
                                break;
                            case "SPECMSSG":
                                break;
                            case "PSTLINFO":
                                break;
                            case "THRDPRTY":
                                break;
                            case "BILLINST":
                                break;
                            case "BILL_END":
                                break;
                        
                        }
                    }
                    count++;
                    Console.WriteLine("Count = {0} ", count);
                }
            }
            _bolContinue = true;
            return count;
        }

    }
    class CPP_billHead
    {
    
       string Record_Type_Label;// 8 
	   string Bill_Number; //6  
	   string Bill_Type;// 2 
	   string Number_Copies;// numeric2, 0 
	   string Bill_Print_Date;// 11 
	   string Bill_Print_Time;// 8 
	   string Billing_Date;// 11 
	   string Address_Name;// 40 
	   string Address_Line1;// 30 
	   string Address_Line2;// 50 
	   string Address_Line3;// 30 
	   string Address_Line4;// 30 
	   string Address_Line5;// 13 
/*	        City 20 
	        State 3 
	        Zip_Code 11 
	        Nation 28 
	        Account_Number 14 
	        Bill_Cycle_Code 7 
	        Unused numeric6, 0 
	        Acct_Check_Digit 1 
	        Unused1 1 
	        Amt_Check_Digit 1 
	        Unused2 4 
	        Delinquency_Flag 1 
	        Suppress_Paper_Bill 1 
	        Insert_Select_1 1 
	        Insert_Select_2 1 
	        Message_File_ID 6 
	        Unused3 14 
	        Delinquency_Cut_Off_Date 10 
 * */
	        /*
             Fileid int 
	        IsSupresses bit 
	        RecordKey bigint  
	        Message_Text 255
             */ 
    }


}
