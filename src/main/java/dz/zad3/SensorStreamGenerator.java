package dz.zad3;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class SensorStreamGenerator {

    public static final int WAIT_PERIOD_IN_MILLISECONDS = 1;
    public static final int PORT = 10002;

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: SensorStreamGenerator <input file>");
            System.exit(-1);
        }

        System.out.println("Waiting for client connection");

        try (ServerSocket serverSocket = new ServerSocket(PORT);
             Socket clientSocket = serverSocket.accept()) {

            System.out.println("Connection successful");
            PrintWriter out =
                    new PrintWriter(
                            new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8), true);

            Stream<String> lines = Files.lines(Paths.get(args[0]));

            lines.forEach(line -> {
                out.println(line);
                try {
                    Thread.sleep(WAIT_PERIOD_IN_MILLISECONDS);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            });

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}