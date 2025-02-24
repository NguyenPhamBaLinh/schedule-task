package rmhub.mod.schedule.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import rmhub.mod.schedule.service.LambdaRunnerService;
import java.io.IOException;

@RestController
@RequestMapping("/lambda")
public class LambdaController {

    private final LambdaRunnerService lambdaRunnerService;

    public LambdaController(LambdaRunnerService lambdaRunnerService) {
        this.lambdaRunnerService = lambdaRunnerService;
    }

    @GetMapping("/run")
    public String runLambda() {
        try {
            return lambdaRunnerService.runLambda();
        } catch (IOException e) {
            return "Error running Lambda: " + e.getMessage();
        }
    }
}

