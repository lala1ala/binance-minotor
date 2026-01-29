# 币安监控系统 (GitHub Actions 版) - 部署指南

这是一个完全免费的云端部署方案。

## 1. 准备工作

### 获取 Firebase 密钥 (Service Account Key)
1. 访问 [Firebase 控制台](https://console.firebase.google.com/)。
2. 进入 **Project Settings (项目设置)** -> **Service accounts (服务账号)**。
3. 点击 **Generate new private key (生成新的私钥)**。
4. 你会下载到一个 `.json` 文件。**请妥善保管这个文件，不要发给任何人。**
5. 用记事本打开这个 JSON 文件，复制里面的**全部内容**。

## 2. GitHub 设置

1. **创建仓库**: 在 GitHub 上创建一个新的仓库 (Repository)，建议设为 **Private (私有)**。
2. **上传代码**: 将本目录 (`binance_monitor_gha`) 下的所有文件上传到该仓库。
   - 确保 `.github/workflows/monitor.yml` 路径正确。
3. **添加 Secrets**:
   - 进入仓库的 **Settings (设置)** -> **Secrets and variables** -> **Actions**。
   - 点击 **New repository secret**，添加以下三个变量：

| Name | Secret | 说明 |
| :--- | :--- | :--- |
| `TELEGRAM_BOT_TOKEN` | (你的 Bot Token) | 原配置中有 (`8577...`) |
| `TELEGRAM_CHAT_ID` | (你的频道 ID) | 原配置中有 (`-100...`) |
| `FIREBASE_CREDENTIALS` | (刚才复制的 JSON 内容) | **整段粘贴** |

## 3. 验证运行

1. 进入仓库的 **Actions** 标签页。
2. 你应该能看到 "Binance Monitor Task" 工作流。
3. 点击左侧的 "Binance Monitor Task"，然后点击右侧的 **Run workflow** 按钮进行手动测试。
4. 如果变成绿色 ✅，说明配置成功！之后它会自动每30分钟运行一次。

## 常见问题
- **找不到 Actions 标签页？** 确保 `.github/workflows/monitor.yml` 文件存在且在正确的位置。
- **报错 `firebase_admin.exceptions`?** 检查 `FIREBASE_CREDENTIALS` 是否复制完整，必须是合法的 JSON 格式。
