package ibm_wmq_messeage_getter;

/**
 *
 * @author theanh@ilovex.co.jp
 */
class MyMessage {

    private String folderName;
    private int seqNo;
    private String ifFileName;
    private String kaiInKey;
    private String item01;
    private String item02;
    private String item03;
    private String insertDate;
    private String sendStatus;
    private String updateDate;
    private String updateProgram;
    private String argument;

    public MyMessage(String folderName, int seqNo, String ifFileName, String kaiInKey, String item01, String item02, String item03, String insertDate, String sendStatus, String updateDate, String updateProgram, String argument) {
        this.folderName = folderName;
        this.seqNo = seqNo;
        this.ifFileName = ifFileName;
        this.kaiInKey = kaiInKey;
        this.item01 = item01;
        this.item02 = item02;
        this.item03 = item03;
        this.insertDate = insertDate;
        this.sendStatus = sendStatus;
        this.updateDate = updateDate;
        this.updateProgram = updateProgram;
        this.argument = argument;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public int getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(int seqNo) {
        this.seqNo = seqNo;
    }

    public String getIfFileName() {
        return ifFileName;
    }

    public void setIfFileName(String ifFileName) {
        this.ifFileName = ifFileName;
    }

    public String getKaiInKey() {
        return kaiInKey;
    }

    public void setKaiInKey(String kaiInKey) {
        this.kaiInKey = kaiInKey;
    }

    public String getItem01() {
        return item01;
    }

    public void setItem01(String item01) {
        this.item01 = item01;
    }

    public String getItem02() {
        return item02;
    }

    public void setItem02(String item02) {
        this.item02 = item02;
    }

    public String getItem03() {
        return item03;
    }

    public void setItem03(String item03) {
        this.item03 = item03;
    }

    public String getInsertDate() {
        return insertDate;
    }

    public void setInsertDate(String insertDate) {
        this.insertDate = insertDate;
    }

    public String getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(String sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(String updateDate) {
        this.updateDate = updateDate;
    }

    public String getUpdateProgram() {
        return updateProgram;
    }

    public void setUpdateProgram(String updateProgram) {
        this.updateProgram = updateProgram;
    }

    public String getArgument() {
        return argument;
    }

    public void setArgument(String argument) {
        this.argument = argument;
    }
}
