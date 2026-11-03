/**
 * radar-cron —— 给 binance-minotor 提供「不丢班」的外部定时器。
 *
 * 为什么需要它
 *   GitHub Actions 的 schedule 是尽力而为（best-effort）：官方文档明确写了
 *   高负载时会延迟、甚至直接丢弃排队中的任务。本仓库实测交付约 3.7/12。
 *   而 workflow_dispatch 是同步入队、有明确返回码，不会丢。
 *   所以用 Cloudflare 的 Cron Trigger 主动调 dispatch，GitHub 自己的 cron 留作兜底。
 *
 * 两个 trigger（UTC，与 GitHub 的班次错开 15 分钟）
 *   22 *\/2 * * *  → monitor.yml（主报告）    GitHub 自己是 07 分
 *   52 *\/2 * * *  → opps.yml（机会扫描）     GitHub 自己是 37 分
 *
 * 撞车了怎么办
 *   仓库里还有一道 90 分钟幂等闸（main.py 的 REPORT_MIN_GAP_MINUTES）：
 *   距上次成功推送不足 90 分钟就整轮跳过，所以两边都触发也不会推两条。
 *
 * 必需的环境变量（在 Worker 的 Settings → Variables and Secrets 里配）
 *   GH_TOKEN  fine-grained PAT，仅本仓库，权限只需 Actions: Read and write
 *   OWNER     lala1ala
 *   REPO      binance-minotor
 */

const WORKFLOWS = {
  '22 */2 * * *': 'monitor.yml',
  '52 */2 * * *': 'opps.yml',
};

async function dispatch(env, workflow) {
  const { OWNER: owner, REPO: repo, GH_TOKEN: token } = env;
  if (!owner || !repo || !token) {
    throw new Error('缺少 OWNER / REPO / GH_TOKEN 环境变量');
  }

  const url =
    `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow}/dispatches`;

  let lastErr = '';
  // 最多试 3 次：GitHub API 偶发 5xx，重试一次基本就过
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
          'User-Agent': 'radar-cron',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ ref: 'main' }),
      });

      // 成功是 204 No Content
      if (res.status === 204) {
        console.log(`OK ${workflow} dispatched (attempt ${attempt})`);
        return `OK ${workflow}`;
      }

      lastErr = `${res.status} ${await res.text()}`;
      console.error(`FAIL ${workflow} attempt ${attempt}: ${lastErr}`);
    } catch (e) {
      lastErr = String(e);
      console.error(`ERR ${workflow} attempt ${attempt}: ${lastErr}`);
    }

    if (attempt < 3) {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  throw new Error(`dispatch ${workflow} failed: ${lastErr}`);
}

export default {
  // Cron 触发入口
  async scheduled(event, env) {
    const workflow = WORKFLOWS[event.cron];
    if (!workflow) {
      console.warn(`未知的 cron 表达式: ${event.cron}`);
      return;
    }
    await dispatch(env, workflow);
  },

  // HTTP 入口，只用来手动验证：
  //   https://<你的worker>.workers.dev/?workflow=monitor.yml
  async fetch(request, env) {
    const u = new URL(request.url);
    const workflow = u.searchParams.get('workflow') || 'monitor.yml';
    try {
      const r = await dispatch(env, workflow);
      return new Response(r + '\n', {
        status: 200,
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      });
    } catch (e) {
      return new Response('FAIL ' + e.message + '\n', {
        status: 500,
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      });
    }
  },
};
