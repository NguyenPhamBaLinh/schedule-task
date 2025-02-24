package rmhub.mod.schedule.service;

import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class LambdaRunnerService {

    private static final String LAMBDA_FILE_PATH = "src/main/lambda/insert_data_direct_to_athena.py";

    public String runLambda() throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("python3", LAMBDA_FILE_PATH);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        StringBuilder output = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        return output.toString();
    }
}

