using System.IO;
using System.Text;

namespace Arie.DAO
{
    class FileManagementSystem
    {
        string message = string.Empty;

        /*      CHECK FOR THE FOLDER ALREADY EXISTS OR NOT      */
        public void checkForFolderExistsOrNot(string folderPath, string fileName, string textContent)
        {
            try
            {
                if (!string.IsNullOrEmpty(folderPath)
                    && !string.IsNullOrEmpty(fileName))
                {
                    if (Directory.Exists(folderPath))
                    {
                        checkForFileExistsOrNot(folderPath + @"\" + fileName + ".txt", textContent);
                    }
                    else
                    {
                        Directory.CreateDirectory(folderPath);
                        checkForFileExistsOrNot(folderPath + @"\" + fileName + ".txt", textContent);
                    }
                }
            }
            catch (IOException ioe)
            {
                message = "Inside checkForFolderExistsOrNot()..\n" + ioe.ToString();
            }
        }

        /*      CHECK FOR THE FILE ALREADY EXISTS OR NOT      */
        public void checkForFileExistsOrNot(string filePath, string myMessageContent)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    appendFileContent(filePath, myMessageContent);
                }
                else
                {
                    writeFileContent(myMessageContent + "\n", filePath, 1);
                }
            }
            catch (IOException ioe)
            {
                message = "Inside checkForFileExistsOrNot()..\n" + ioe.ToString();
            }
        }

        /*      APPENDING THE FILE CONTENT      */
        public void appendFileContent(string filePath, string fileContent)
        {
            try
            {
                StreamWriter streamWriter = File.AppendText(filePath);
                //streamWriter.WriteLine(fileContent);
                streamWriter.Write(fileContent);
                streamWriter.Close();
            }
            catch (IOException ioe)
            {
                message = "appendFileContent()..\n" + ioe.ToString();
            }
        }

        /*      CREATING THE FOLDER NAME      */
        public bool createFolder(string folderName)
        {
            try
            {
                return Directory.CreateDirectory(folderName).Exists;
            }
            catch (IOException ioe)
            {
                message = "Inside createFolder()..\n" + ioe.ToString();
                return false;
            }
        }

        /*      READ THE CONTENT FOR THE GIVEN FILE NAME    */
        public string readFileContent(string fileNameWithTypeAndLocation, int fileReadMode)
        {
            try
            {
                string str = string.Empty;
                switch (fileReadMode)
                {
                    case 1:     //////          READING DATA USING StreamReader     //
                        StreamReader streamReadObject = new StreamReader(fileNameWithTypeAndLocation);
                        str = streamReadObject.ReadToEnd().ToString();
                        streamReadObject.Close();
                        break;
                    case 2:     //////          READING DATA USING File             //
                        str = File.ReadAllText(fileNameWithTypeAndLocation);
                        break;
                    case 3:     //////          READING DATA USING FileStream       //
                        FileStream fileStreamObject = new FileStream(fileNameWithTypeAndLocation, FileMode.Open);
                        byte[] byteArray = new byte[(int)fileStreamObject.Length];
                        int nBytesRead = fileStreamObject.Read(byteArray, 0, (int)fileStreamObject.Length);
                        str = Encoding.ASCII.GetString((byteArray));
                        fileStreamObject.Close();
                        break;
                    case 4:     //////          READING DATA USING TextReader       //
                        TextReader textReader = File.OpenText(fileNameWithTypeAndLocation);
                        str = textReader.ReadToEnd();
                        textReader.Close();
                        break;
                    case 5:     //////          READING DATA USING BinaryReader     //
                        BinaryReader binaryReader = new BinaryReader(File.Open(fileNameWithTypeAndLocation, FileMode.Open));
                        str = binaryReader.ReadString();
                        binaryReader.Close();
                        break;
                    default:
                        break;
                }
                return str;
            }
            catch (IOException ioe)
            {
                message = "Inside readFileContent()..\n" + ioe.ToString();
                return null;
            }
        }

        /*      WRITE THE NEW CONTENT FOR THE GIVEN FILE NAME WITH PATH LOCATION    */
        public void writeFileContent(string fileContent, string fileNameWithTypeAndLocation, int fileWriteMode)
        {
            try
            {
                string str = string.Empty;
                switch (fileWriteMode)
                {
                    case 1:    //////          WRITING DATA USING StreamWriter      //
                        StreamWriter streamWriteObject = new StreamWriter(fileNameWithTypeAndLocation);
                        streamWriteObject.Write(fileContent + str);
                        streamWriteObject.Close();
                        break;
                    case 2:     //////          WRITING DATA USING File             //
                        File.WriteAllText(fileNameWithTypeAndLocation, fileContent + str);
                        break;
                    case 3:     //////          WRITING DATA USING FileStream       //
                        byte[] byteData = null;
                        byteData = Encoding.ASCII.GetBytes(fileContent + str);
                        FileStream fileStreamObject = new FileStream(fileNameWithTypeAndLocation, FileMode.Append);
                        fileStreamObject.Write(byteData, 0, byteData.Length);
                        fileStreamObject.Close();
                        break;
                    case 4:     //////          READING DATA USING TextWriter       //
                        TextWriter textWriter = File.CreateText(fileNameWithTypeAndLocation);
                        textWriter.Write(fileContent + str);
                        textWriter.Close();
                        break;
                    case 5:     //////          READING DATA USING BinaryWriter     //
                        BinaryWriter binaryWriter = new BinaryWriter(File.Open(fileNameWithTypeAndLocation, FileMode.Create));
                        binaryWriter.Write(fileContent + str);
                        binaryWriter.Close();
                        break;
                    default:
                        break;
                }
            }
            catch (IOException ioe)
            {
                message = "Inside writeFileContent()..\n" + ioe.ToString();
            }
        }

        public bool deleteFile(string deleteFilePath)
        {
            try
            {
                File.Delete(deleteFilePath);
                message = "FILE DELETED SUCCESSFULLY..\n";
                return true;
            }
            catch (IOException ioe)
            {
                message = "deleteFile()..\n" + ioe.ToString();
                return false;
            }
        }

        public bool copyFileTo(string sourceFilePath, string destinationFilePath)
        {
            try
            {
                if (File.Exists(sourceFilePath))
                {
                    File.Copy(sourceFilePath, destinationFilePath);
                    message = "FILE COPIED FROM [" + sourceFilePath + "] TO [" + destinationFilePath + "]";
                    return true;
                }
                else
                {
                    message = "FILE NOT EXISTS IN THE [" + sourceFilePath + "] PATH";
                    return false;
                }
            }
            catch (IOException ioe)
            {
                message = "Inside copyFileTo()..\n" + ioe.ToString();
                return false;
            }
        }

        public bool moveFileTo(string sourceFilePath, string destinationFilePath)
        {
            try
            {
                if (File.Exists(sourceFilePath))
                {
                    File.Move(sourceFilePath, destinationFilePath);
                    message = "FILE MOVED FROM [" + sourceFilePath + "] TO [" + destinationFilePath + "]";
                    return true;
                }
                else
                {
                    message = "FILE NOT EXISTS IN THE [" + sourceFilePath + "] PATH";
                    return false;
                }
            }
            catch (IOException ioe)
            {
                message = "Inside moveFileTo()..\n" + ioe.ToString();
                return false;
            }
        }
    }
}