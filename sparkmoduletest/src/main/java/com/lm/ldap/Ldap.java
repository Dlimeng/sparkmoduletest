package com.lm.ldap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.util.Hashtable;

/**
 * @Classname Ldap
 * @Description TODO
 * @Date 2020/5/9 17:38
 * @Created by limeng
 */
public class Ldap {
    public static void main(String[] args) {
        Hashtable<String, String> env = new Hashtable<>();
        String baseDN="dc=knowlegene,dc=com";
        String url="ldap://192.168.200.31:389/";
        String password="limeng";

        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        env.put(Context.SECURITY_PRINCIPAL, "uid=limeng,ou=linkis,dc=knowlegene,dc=com");
        env.put(Context.SECURITY_CREDENTIALS, "limeng");
        LdapContext ldapCtx = null;
        try {
            ldapCtx =  new InitialLdapContext(env, null);
        } catch (NamingException e) {
            e.printStackTrace();
        }finally {
            if(ldapCtx != null) {
                try {
                    ldapCtx.close();
                } catch (NamingException e) {
                }
            }
        }
    }
}
