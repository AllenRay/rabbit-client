package cn.rabbit.test;

import java.io.Serializable;

/**
 * Created by Administrator on 2015/12/9.
 */
public class Email implements Serializable {

    private String emailTempate;

    private String emailContent;

    public String getEmailTempate() {
        return emailTempate;
    }

    public void setEmailTempate(String emailTempate) {
        this.emailTempate = emailTempate;
    }

    public String getEmailContent() {
        return emailContent;
    }

    public void setEmailContent(String emailContent) {
        this.emailContent = emailContent;
    }
}
