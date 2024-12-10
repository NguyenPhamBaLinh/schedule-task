package rmhub.mod.schedule.aop;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class PrivateAllRepo {
    private static final String GITHUB_TOKEN = "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"; // Thay bằng token của bạn
    private static final String GITHUB_USER = "cuongnv99"; // Thay bằng tên người dùng GitHub của bạn

    public static void main(String[] args) throws IOException {
        // Lấy danh sách repository của người dùng
        JSONArray repos = getRepositories();

        if (repos != null) {
            for (int i = 0; i < repos.size(); i++) {
                // Cập nhật repository thành private
                JSONObject repo = repos.getJSONObject(i);
                String repoName = repo.getString("name");
                makeRepoPrivate(repoName);
            }
        }
    }

    // Lấy danh sách repository của người dùng
    private static JSONArray getRepositories() throws IOException {
        URL url = new URL("https://api.github.com/users/" + GITHUB_USER + "/repos");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", "token " + GITHUB_TOKEN);

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder response = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // Parse JSON response và trả về mảng các repository
        return JSONArray.parseArray(response.toString());
    }

    // Cập nhật repository thành private
    private static void makeRepoPrivate(String repoName) throws IOException {
        URL url = new URL("https://api.github.com/repos/" + GITHUB_USER + "/" + repoName);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", "token " + GITHUB_TOKEN);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        // Tạo JSON body để thay đổi quyền truy cập thành private
        JSONObject jsonBody = new JSONObject();
        jsonBody.put("private", true);

        connection.getOutputStream().write(jsonBody.toString().getBytes());

        // Kiểm tra nếu request thành công
        if (connection.getResponseCode() == 200) {
            System.out.println("Repository " + repoName + " đã được thay đổi thành private.");
        } else {
            System.out.println("Lỗi khi thay đổi repository " + repoName + ": " + connection.getResponseMessage());
        }
    }
}

