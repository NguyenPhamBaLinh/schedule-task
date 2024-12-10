package rmhub.mod.schedule.aop;

import com.alibaba.fastjson.JSONObject;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class PushALLRepo {
    // GitHub API token và tên người dùng GitHub
    private static final String GITHUB_TOKEN = "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"; // Thay bằng token của bạn
    private static final String GITHUB_USER = "cuongnv99"; // Thay bằng tên người dùng GitHub của bạn
    private static final String GITHUB_API_URL = "https://api.github.com/user/repos";  // Cập nhật đúng URL


    // Thư mục cha chứa các project
    private static final String PARENT_DIR = "C:\\Users\\CuongNV37\\Cuongnv99\\MyGitCode\\bankcode"; // Thay bằng đường dẫn thư mục cha

    public static void main(String[] args) throws Exception {
        File parentDir = new File(PARENT_DIR);
        // Lấy danh sách tất cả các thư mục con trong thư mục cha
        String[] projects = parentDir.list((dir, name) -> new File(dir, name).isDirectory());

        if (projects != null) {
            for (String projectName : projects) {
                createGitHubRepo(projectName);
                pushProjectToGitHub(projectName);
            }
        }
    }

    // Tạo repository trên GitHub thông qua API
    private static void createGitHubRepo(String projectName) throws IOException {
        URL url = new URL(GITHUB_API_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", "token " + GITHUB_TOKEN);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        // Tạo JSON body cho request
        JSONObject jsonBody = new JSONObject();
        jsonBody.put("name", projectName);
        jsonBody.put("auto_init", true); // Tạo repository với commit ban đầu
        jsonBody.put("private", false); // Có thể đặt true nếu muốn repo riêng tư

        connection.getOutputStream().write(jsonBody.toString().getBytes());

        // Kiểm tra nếu request thành công
        if (connection.getResponseCode() == 201) {
            System.out.println("Repository " + projectName + " đã được tạo trên GitHub.");
        } else {
            System.out.println("Lỗi khi tạo repository " + projectName + ": " + connection.getResponseMessage());
        }
    }

    // Push project lên GitHub
    private static void pushProjectToGitHub(String projectName) throws Exception {
        File projectDir = new File(PARENT_DIR, projectName);

        // Khởi tạo Git repo cho thư mục
        Git git = Git.init().setDirectory(projectDir).call();

        // Thêm remote URL của GitHub repository
        String remoteUrl = "https://github.com/" + GITHUB_USER + "/" + projectName + ".git";
        git.remoteAdd().setName("origin").setUri(new URIish(remoteUrl)).call();

        // Thêm và commit tất cả các file
        git.add().addFilepattern(".").call();
        git.commit().setMessage("Initial commit").call();

        // Push mã nguồn lên GitHub
        git.push()
                .setCredentialsProvider(new UsernamePasswordCredentialsProvider(GITHUB_USER, GITHUB_TOKEN))
                .call();

        System.out.println("Đã đẩy mã nguồn của " + projectName + " lên GitHub.");
    }
}
