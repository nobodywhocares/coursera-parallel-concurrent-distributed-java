package edu.coursera.distributed;

import java.io.BufferedReader;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {

    protected final static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     * class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     * should be a fatal error, your file server implementation is not expected
     * to ever throw IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {

            // TODO Delete this once you start working on your solution.
            // throw new UnsupportedOperationException();
            // TODO 1) Use socket.accept to get a Socket object

            /*
             * TODO 2) Using Socket.getInputStream(), parse the received HTTP
             * packet. In particular, we are interested in confirming this
             * message is a GET and parsing out the path to the file we are
             * GETing. Recall that for GET HTTP packets, the first line of the
             * received packet will look something like:
             *
             *     GET /path/to/file HTTP/1.1
             */
             /*
             * TODO 3) Using the parsed path to the target file, construct an
             * HTTP reply and write it to Socket.getOutputStream(). If the file
             * exists, the HTTP reply should be formatted as follows:
             *
             *   HTTP/1.0 200 OK\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *   FILE CONTENTS HERE\r\n
             *
             * If the specified file does not exist, you should return a reply
             * with an error code 404 Not Found. This reply should be formatted
             * as:
             *
             *   HTTP/1.0 404 Not Found\r\n
             *   Server: FileServer\r\n
             *   \r\n
             *
             * Don't forget to close the output stream.
             */
            Socket s = socket.accept();
            // New thread instead of executor pool was too slow to pass performance test
//            Thread thread = new Thread(() -> {
            executor.submit(() -> {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String line = reader.readLine();
                    assert line != null;
                    assert line.startsWith("GET");
                    PCDPPath path = new PCDPPath(line.split(" ")[1]);
                    // Java's new construct "try-with-resources" 
                    // will auto flush/close the stream
                    try (PrintWriter writer = new PrintWriter(s.getOutputStream())) {
                        String fileContent;
                        if ((fileContent = fs.readFile(path)) != null) {
                            writer.write("HTTP/1.0 200 OK\r\n");
                            writer.write("Server: FileServer\r\n");
                            writer.write("\r\n");
                            writer.write(fileContent);
                        } else {
                            writer.write("HTTP/1.0 404 Not Found\r\n");
                            writer.write("Server: FileServer\r\n");
                            writer.write("\r\n");
                        }
                    }
                } catch (IOException ex) {
                    Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, "Failed to process client request", ex);
                } finally {
                    if (null != reader) {
                        try {
                            reader.close();
                        } catch (IOException ex1) {
                            Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, "Failed to close client socket reader", ex1);
                        }
                    }
                }
            });
            //thread.start();
        }
    }
}
