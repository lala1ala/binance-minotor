/**
 * radar-cron —— 给 binance-minotor 提供「不丢班」的外部定时器。
 *
 * 为什么需要它
 *   GitHub Actions 的 schedule 是尽力而为（best-effort）：官方文档明确写了
 *   高负载时会延迟、甚至直接丢弃排队中的任务。本仓库实测交付约 3.7/12。
 *   而 workflow_dispatch 是同步入队、有明确返回码，不会丢。
 *   所以用 Cloudflare 的 Cron Trigger 主动调 dispatch，GitHub 自己的 cron 留作兜底。
 *
 * 三个 trigger（UTC，都比 GitHub 自己那班晚 15~22 分钟，靠幂等闸去重）
 *   22 *\/6 * * *  → monitor.yml（主报告）    GitHub 自己是 07 分，每 6 小时
 *   52 *\/2 * * *  → opps.yml（机会扫描）     GitHub 自己是 37 分，仍每 2 小时
 *   22 1  * * *    → warmup.yml（升温报告）   GitHub 自己是 00 分，每天 1 次
 *
 * 注意主报告是「每 6 小时」而不是每 2 小时：用户 2026-11-03 明确 2 小时一条太频繁，
 * 每天 4 条即可。opps 保持 2 小时是因为它每轮只写表、不推送（日报每天 1 条、
 * 爆空仅首次命中推），不会造成骚扰。
 *
 * 撞车了怎么办
 *   仓库里还有一道 90 分钟幂等闸（main.py 的 REPORT_MIN_GAP_MINUTES）：
 *   距上次成功推送不足 90 分钟就整轮跳过，所以两边都触发也不会推两条。
 *   （GitHub :07 与 CF :22 只差 15 分钟，靠这道闸门去重。）
 *
 *   ⚠️ warmup 例外：它跑在 main.py 那道闸**之前**（warmup 分支自己 return），
 *   所以用的是另一道闸 —— 「当天已发过就跳过」（opps_state 的 last_warmup_date）。
 *   否则每天会出现两条一模一样的升温报告。
 *
 * 必需的环境变量（在 Worker 的 Settings → Variables and Secrets 里配）
 *   GH_TOKEN  fine-grained PAT，仅本仓库，权限只需 Actions: Read and write
 *   OWNER     lala1ala
 *   REPO      binance-minotor
 */

const WORKFLOWS = {
  '22 */6 * * *': 'monitor.yml',
  '52 */2 * * *': 'opps.yml',
  '22 1 * * *': 'warmup.yml',
};

/**
 * 把 Cloudflare 回传的 cron 表达式规范化后再匹配。
 *
 * 为什么不能直接 WORKFLOWS[event.cron]：
 *   dashboard 的可视化编辑器可能把 `... *\/6 ...` 展开成 `... 0,6,12,18 ...`，
 *   或者表达式里多一个空格。一旦字符串不完全相等，精确匹配就会落空 ——
 *   而落空的后果是「只打一行日志、什么都不触发」，从外面看就是「配好了但不动」。
 *   这类静默失效最难查，所以这里做两层容错。
 */
function normalizeCron(expr) {
  return String(expr || '').trim().split(/\s+/).join(' ');
}

function pickWorkflow(expr) {
  const key = normalizeCron(expr);
  if (WORKFLOWS[key]) return WORKFLOWS[key];

  // 第一层容错：按「分」+「时」两个字段判断，忽略写法差异
  const [minute, hour] = key.split(' ');
  // warmup 是单小时（每天一次），必须先判，否则会落到下面的通配规则里
  if (minute === '22' && ['1', '01'].includes(hour)) return 'warmup.yml';
  if (minute === '22' && ['*/6', '0,6,12,18'].includes(hour)) return 'monitor.yml';
  if (minute === '52' && ['*/2', '0,2,4,6,8,10,12,14,16,18,20,22'].includes(hour)) {
    return 'opps.yml';
  }
  return null;
}

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
    const mapped = pickWorkflow(event.cron);
    // 实在认不出来也别静默丢掉：默认跑主报告，并把原始表达式打进日志，
    // 这样即使表达式填错，也不会出现「配好了但永远不动」。
    const workflow = mapped || 'monitor.yml';
    if (!mapped) {
      console.error(
        `未知的 cron 表达式: "${event.cron}"（规范化后 "${normalizeCron(event.cron)}"）` +
        `→ 兜底派发 ${workflow}，请检查 Cron Trigger 应为 ` +
        `22 */6（主报告）、52 */2（机会扫描）或 22 1（升温）之一`
      );
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
