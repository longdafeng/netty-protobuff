package us.longda.tools.test;

import us.longda.tools.client.MessageClient;
import us.longda.tools.common.ByteConvert;
import us.longda.tools.server.MessageServer;

public final class TestNio {
    
    public static void startClient(final String[] args) {
        

        Thread client = new Thread(new Runnable() {
            
            public void run() {
                // TODO Auto-generated method stub
                try {
                    Thread.sleep(1000);
                    MessageClient.main(args);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        
        client.start();
    }
    
    public static void startServer(final String[] args) {
        

        Thread server = new Thread(new Runnable() {
            
            public void run() {
                // TODO Auto-generated method stub
                try {
                    MessageServer.main(args);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        
        server.start();
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        startClient(args);
        
        startServer(args);
        
        while(true) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
}
