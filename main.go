package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants & coin map
// ─────────────────────────────────────────────────────────────────────────────

const cgBase = "https://api.coingecko.com/api/v3"

// Maps uppercase ticker symbol → CoinGecko coin ID.
var coinIDMap = map[string]string{
	"BTC":   "bitcoin",
	"ETH":   "ethereum",
	"BNB":   "binancecoin",
	"SOL":   "solana",
	"ADA":   "cardano",
	"XRP":   "ripple",
	"DOT":   "polkadot",
	"DOGE":  "dogecoin",
	"AVAX":  "avalanche-2",
	"MATIC": "matic-network",
	"LINK":  "chainlink",
	"UNI":   "uniswap",
	"LTC":   "litecoin",
	"BCH":   "bitcoin-cash",
	"ATOM":  "cosmos",
	"XLM":   "stellar",
	"VET":   "vechain",
	"FIL":   "filecoin",
	"TRX":   "tron",
	"ETC":   "ethereum-classic",
	"ALGO":  "algorand",
	"XMR":   "monero",
	"AAVE":  "aave",
	"SHIB":  "shiba-inu",
	"SAND":  "the-sandbox",
	"MANA":  "decentraland",
	"NEAR":  "near",
	"FTM":   "fantom",
	"HBAR":  "hedera-hashgraph",
	"ICP":   "internet-computer",
	"OP":    "optimism",
	"ARB":   "arbitrum",
	"SUI":   "sui",
	"INJ":   "injective-protocol",
	"TON":   "the-open-network",
	"APT":   "aptos",
	"TIA":   "celestia",
	"SEI":   "sei-network",
	"USDT":  "tether",
	"USDC":  "usd-coin",
	"DAI":   "dai",
	"PEPE":  "pepe",
	"WIF":   "dogwifcoin",
}

// ─────────────────────────────────────────────────────────────────────────────
// CoinGecko response types
// ─────────────────────────────────────────────────────────────────────────────

type PriceData struct {
	USD          float64 `json:"usd"`
	USD24hChange float64 `json:"usd_24h_change"`
	USDMarketCap float64 `json:"usd_market_cap"`
	USD24hVol    float64 `json:"usd_24h_vol"`
}

type CoinMarket struct {
	ID                 string  `json:"id"`
	Symbol             string  `json:"symbol"`
	Name               string  `json:"name"`
	CurrentPrice       float64 `json:"current_price"`
	MarketCap          float64 `json:"market_cap"`
	MarketCapRank      int     `json:"market_cap_rank"`
	PriceChangePercent float64 `json:"price_change_percentage_24h"`
	TotalVolume        float64 `json:"total_volume"`
}

type SearchResult struct {
	Coins []struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
	} `json:"coins"`
}

type MarketChart struct {
	Prices [][]float64 `json:"prices"`
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory state (alerts + portfolio)
// ─────────────────────────────────────────────────────────────────────────────

type Alert struct {
	CoinID    string
	Symbol    string
	Condition string // "above" | "below"
	Target    float64
	ChatID    int64
}

type PortfolioEntry struct {
	CoinID string
	Symbol string
	Amount float64
}

var (
	mu         sync.RWMutex
	alerts     []Alert
	portfolios = make(map[int64][]PortfolioEntry)
)

// ─────────────────────────────────────────────────────────────────────────────
// HTTP helper
// ─────────────────────────────────────────────────────────────────────────────

func cgGet(path string, params url.Values) ([]byte, error) {
	u := cgBase + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "CryptoBot/1.0")
	client := &http.Client{Timeout: 12 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited — please try again in a moment")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API error %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// ─────────────────────────────────────────────────────────────────────────────
// Coin resolution
// ─────────────────────────────────────────────────────────────────────────────

// resolveCoinID maps a user-supplied ticker or name to a CoinGecko ID.
func resolveCoinID(input string) (id, symbol string, err error) {
	upper := strings.ToUpper(strings.TrimSpace(input))
	if cgID, ok := coinIDMap[upper]; ok {
		return cgID, upper, nil
	}
	// Fall back to CoinGecko search
	data, e := cgGet("/search", url.Values{"query": {input}})
	if e != nil {
		return "", "", fmt.Errorf("coin not found: %s", input)
	}
	var sr SearchResult
	if e := json.Unmarshal(data, &sr); e != nil || len(sr.Coins) == 0 {
		return "", "", fmt.Errorf("no results for '%s'", input)
	}
	return sr.Coins[0].ID, strings.ToUpper(sr.Coins[0].Symbol), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// CoinGecko API calls
// ─────────────────────────────────────────────────────────────────────────────

func fetchPrice(coinID string) (*PriceData, error) {
	data, err := cgGet("/simple/price", url.Values{
		"ids":                 {coinID},
		"vs_currencies":       {"usd"},
		"include_24hr_change": {"true"},
		"include_market_cap":  {"true"},
		"include_24hr_vol":    {"true"},
	})
	if err != nil {
		return nil, err
	}
	var result map[string]PriceData
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	pd, ok := result[coinID]
	if !ok {
		return nil, fmt.Errorf("no data for %s", coinID)
	}
	return &pd, nil
}

func fetchTopCoins(n int) ([]CoinMarket, error) {
	data, err := cgGet("/coins/markets", url.Values{
		"vs_currency": {"usd"},
		"order":       {"market_cap_desc"},
		"per_page":    {strconv.Itoa(n)},
		"page":        {"1"},
		"sparkline":   {"false"},
	})
	if err != nil {
		return nil, err
	}
	var coins []CoinMarket
	return coins, json.Unmarshal(data, &coins)
}

func fetchChart(coinID string, days int) ([]float64, error) {
	data, err := cgGet(fmt.Sprintf("/coins/%s/market_chart", coinID), url.Values{
		"vs_currency": {"usd"},
		"days":        {strconv.Itoa(days)},
		"interval":    {"daily"},
	})
	if err != nil {
		return nil, err
	}
	var chart MarketChart
	if err := json.Unmarshal(data, &chart); err != nil {
		return nil, err
	}
	prices := make([]float64, len(chart.Prices))
	for i, p := range chart.Prices {
		prices[i] = p[1]
	}
	return prices, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Formatting helpers
// ─────────────────────────────────────────────────────────────────────────────

func fmtUSD(v float64) string {
	switch {
	case v >= 1_000_000_000:
		return fmt.Sprintf("$%.2fB", v/1_000_000_000)
	case v >= 1_000_000:
		return fmt.Sprintf("$%.2fM", v/1_000_000)
	case v >= 1:
		return fmt.Sprintf("$%.4f", v)
	case v >= 0.0001:
		return fmt.Sprintf("$%.6f", v)
	default:
		return fmt.Sprintf("$%.10f", v)
	}
}

func trendEmoji(pct float64) string {
	if pct >= 5 {
		return "🚀"
	}
	if pct > 0 {
		return "📈"
	}
	if pct >= -5 {
		return "📉"
	}
	return "🔻"
}

func signStr(v float64) string {
	if v >= 0 {
		return "+"
	}
	return ""
}

// sparkline builds an ASCII bar chart from a price slice using 8 Unicode blocks.
func sparkline(prices []float64) string {
	const blocks = "▁▂▃▄▅▆▇█"
	runes := []rune(blocks)
	if len(prices) < 2 {
		return ""
	}
	lo, hi := prices[0], prices[0]
	for _, p := range prices {
		if p < lo {
			lo = p
		}
		if p > hi {
			hi = p
		}
	}
	diff := hi - lo
	if diff == 0 {
		diff = 1
	}
	out := make([]rune, len(prices))
	for i, p := range prices {
		idx := int(math.Round((p - lo) / diff * float64(len(runes)-1)))
		if idx < 0 {
			idx = 0
		} else if idx >= len(runes) {
			idx = len(runes) - 1
		}
		out[i] = runes[idx]
	}
	return string(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Messaging helper
// ─────────────────────────────────────────────────────────────────────────────

func send(ctx context.Context, b *bot.Bot, chatID int64, text string) {
	_, err := b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:    chatID,
		Text:      text,
		ParseMode: models.ParseModeMarkdown,
	})
	if err != nil {
		// Retry without markdown if parsing fails
		_, _ = b.SendMessage(ctx, &bot.SendMessageParams{
			ChatID: chatID,
			Text:   text,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Default handler — routes all messages
// ─────────────────────────────────────────────────────────────────────────────

func defaultHandler(ctx context.Context, b *bot.Bot, update *models.Update) {
	if update.Message == nil || update.Message.Text == "" {
		return
	}
	text := strings.TrimSpace(update.Message.Text)
	chatID := update.Message.Chat.ID
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return
	}

	// Strip @BotUsername suffix from commands (e.g. /price@MyBot → /price)
	cmd := strings.ToLower(parts[0])
	if at := strings.Index(cmd, "@"); at != -1 {
		cmd = cmd[:at]
	}
	args := parts[1:]

	switch cmd {
	case "/start":
		handleStart(ctx, b, chatID)
	case "/help":
		handleHelp(ctx, b, chatID)
	case "/price", "/p":
		handlePrice(ctx, b, chatID, args)
	case "/top":
		n := 10
		if len(args) > 0 {
			if v, err := strconv.Atoi(args[0]); err == nil && v > 0 && v <= 20 {
				n = v
			}
		}
		handleTop(ctx, b, chatID, n)
	case "/chart", "/c":
		handleChart(ctx, b, chatID, args)
	case "/alert":
		handleSetAlert(ctx, b, chatID, args)
	case "/alerts":
		handleListAlerts(ctx, b, chatID)
	case "/delalert":
		handleDelAlert(ctx, b, chatID, args)
	case "/portfolio", "/port":
		handlePortfolio(ctx, b, chatID, args)
	case "/convert":
		handleConvert(ctx, b, chatID, args)
	default:
		send(ctx, b, chatID, "❓ Unknown command. Type /help to see what I can do.")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /start
// ─────────────────────────────────────────────────────────────────────────────

func handleStart(ctx context.Context, b *bot.Bot, chatID int64) {
	send(ctx, b, chatID, `🚀 Welcome to CryptoBot!

Your real-time crypto companion — powered by CoinGecko.

What I can do🛺:
• Live prices & market data
• 7-day price charts
• Price alerts (automatic notifications)
• Portfolio tracker
• Currency converter

Type /help for a full list of commands.`)
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /help
// ─────────────────────────────────────────────────────────────────────────────

func handleHelp(ctx context.Context, b *bot.Bot, chatID int64) {
	send(ctx, b, chatID, `📖 CryptoBot Commands

💰 Prices
/price <symbol> — Live price + 24h stats
/top [n] — Top N coins by market cap (default 10)

📊 Charts
/chart <symbol> — 7-day ASCII price chart

🔔 Alerts
/alert <symbol> above|below <price> — Set alert
/alerts — List your active alerts
/delalert <symbol> — Remove alerts for a coin

💼 Portfolio
/portfolio — View portfolio & total value
/portfolio add <symbol> <amount> — Add holding
/portfolio remove <symbol> — Remove holding

💱 Convert
/convert <amount> <from> to <to>

Examples:
  /price BTC
  /top 5
  /chart ETH
  /alert SOL above 200
  /portfolio add ETH 2.5
  /convert 0.1 BTC to ETH`)
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /price <symbol>
// ─────────────────────────────────────────────────────────────────────────────

func handlePrice(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	if len(args) == 0 {
		send(ctx, b, chatID, "Usage: /price <symbol>\nExample: /price BTC")
		return
	}
	send(ctx, b, chatID, fmt.Sprintf("🔍 Fetching *%s* price...", strings.ToUpper(args[0])))

	coinID, symbol, err := resolveCoinID(args[0])
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	pd, err := fetchPrice(coinID)
	if err != nil {
		send(ctx, b, chatID, "❌ Failed to fetch price: "+err.Error())
		return
	}

	send(ctx, b, chatID, fmt.Sprintf(
		`%s *%s / USD*

💵 Price:       *%s*
%s 24h Change:  *%s%.2f%%*
📊 Market Cap:  *%s*
📦 24h Volume:  *%s*

_⏱ %s UTC_`,
		trendEmoji(pd.USD24hChange), symbol,
		fmtUSD(pd.USD),
		trendEmoji(pd.USD24hChange), signStr(pd.USD24hChange), pd.USD24hChange,
		fmtUSD(pd.USDMarketCap),
		fmtUSD(pd.USD24hVol),
		time.Now().UTC().Format("2006-01-02 15:04:05"),
	))
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /top [n]
// ─────────────────────────────────────────────────────────────────────────────

func handleTop(ctx context.Context, b *bot.Bot, chatID int64, n int) {
	send(ctx, b, chatID, fmt.Sprintf("📊 Fetching top %d coins...", n))
	coins, err := fetchTopCoins(n)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🏆 Top %d Cryptocurrencies\n\n", n))
	for _, c := range coins {
		sb.WriteString(fmt.Sprintf(
			"%d. *%s* (%s)\n   %s  %s%s%.2f%%\n",
			c.MarketCapRank,
			c.Name, strings.ToUpper(c.Symbol),
			fmtUSD(c.CurrentPrice),
			trendEmoji(c.PriceChangePercent),
			signStr(c.PriceChangePercent), c.PriceChangePercent,
		))
	}
	sb.WriteString(fmt.Sprintf("\n⏱ %s UTC", time.Now().UTC().Format("15:04:05")))
	send(ctx, b, chatID, sb.String())
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /chart <symbol>
// ─────────────────────────────────────────────────────────────────────────────

func handleChart(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	if len(args) == 0 {
		send(ctx, b, chatID, "Usage: /chart <symbol>\nExample: /chart BTC")
		return
	}
	send(ctx, b, chatID, fmt.Sprintf("📈 Building 7-day chart for *%s*...", strings.ToUpper(args[0])))

	coinID, symbol, err := resolveCoinID(args[0])
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	prices, err := fetchChart(coinID, 7)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	if len(prices) < 2 {
		send(ctx, b, chatID, "❌ Not enough data to draw a chart.")
		return
	}

	first, last := prices[0], prices[len(prices)-1]
	pct := (last - first) / first * 100
	spark := sparkline(prices)

	// Min / max over the period
	lo, hi := prices[0], prices[0]
	for _, p := range prices {
		if p < lo {
			lo = p
		}
		if p > hi {
			hi = p
		}
	}

	send(ctx, b, chatID, fmt.Sprintf(
		`%s *%s* — 7-Day Chart

%s

📅 Open:  %s
📅 Close: %s
%s  7d:   *%s%.2f%%*
📉 Low:   %s
📈 High:  %s`,
		trendEmoji(pct), symbol,
		spark,
		fmtUSD(first), fmtUSD(last),
		trendEmoji(pct), signStr(pct), pct,
		fmtUSD(lo), fmtUSD(hi),
	))
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /alert <symbol> above|below <price>
// ─────────────────────────────────────────────────────────────────────────────

func handleSetAlert(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	if len(args) < 3 {
		send(ctx, b, chatID, "Usage: /alert <symbol> above|below <price>\nExample: /alert BTC above 100000")
		return
	}
	coinID, symbol, err := resolveCoinID(args[0])
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	cond := strings.ToLower(args[1])
	if cond != "above" && cond != "below" {
		send(ctx, b, chatID, "❌ Condition must be 'above' or 'below'.")
		return
	}
	target, err := strconv.ParseFloat(strings.ReplaceAll(args[2], ",", ""), 64)
	if err != nil || target <= 0 {
		send(ctx, b, chatID, "❌ Invalid price value.")
		return
	}

	mu.Lock()
	alerts = append(alerts, Alert{
		CoinID:    coinID,
		Symbol:    symbol,
		Condition: cond,
		Target:    target,
		ChatID:    chatID,
	})
	mu.Unlock()

	var condIcon string
	if cond == "above" {
		condIcon = "🔼"
	} else {
		condIcon = "🔽"
	}
	send(ctx, b, chatID, fmt.Sprintf(
		"🔔 *Alert set!*\n\nI'll notify you when *%s* goes %s %s %s",
		symbol, condIcon, cond, fmtUSD(target),
	))
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /alerts
// ─────────────────────────────────────────────────────────────────────────────

func handleListAlerts(ctx context.Context, b *bot.Bot, chatID int64) {
	mu.RLock()
	var mine []Alert
	for _, a := range alerts {
		if a.ChatID == chatID {
			mine = append(mine, a)
		}
	}
	mu.RUnlock()

	if len(mine) == 0 {
		send(ctx, b, chatID, "📭 You have no active alerts.\n\nSet one with /alert <symbol> above|below <price>")
		return
	}
	var sb strings.Builder
	sb.WriteString("🔔 *Your Active Alerts*\n\n")
	for i, a := range mine {
		var icon string
		if a.Condition == "above" {
			icon = "🔼"
		} else {
			icon = "🔽"
		}
		sb.WriteString(fmt.Sprintf("%d. *%s* %s %s %s\n", i+1, a.Symbol, icon, a.Condition, fmtUSD(a.Target)))
	}
	sb.WriteString("\nUse /delalert <symbol> to remove.")
	send(ctx, b, chatID, sb.String())
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /delalert <symbol>
// ─────────────────────────────────────────────────────────────────────────────

func handleDelAlert(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	if len(args) == 0 {
		send(ctx, b, chatID, "Usage: /delalert <symbol>\nExample: /delalert BTC")
		return
	}
	_, symbol, err := resolveCoinID(args[0])
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	mu.Lock()
	var remaining []Alert
	removed := 0
	for _, a := range alerts {
		if a.ChatID == chatID && a.Symbol == symbol {
			removed++
		} else {
			remaining = append(remaining, a)
		}
	}
	alerts = remaining
	mu.Unlock()

	if removed == 0 {
		send(ctx, b, chatID, fmt.Sprintf("❌ No alerts found for *%s*.", symbol))
	} else {
		send(ctx, b, chatID, fmt.Sprintf("✅ Removed %d alert(s) for *%s*.", removed, symbol))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /portfolio [add|remove] [args]
// ─────────────────────────────────────────────────────────────────────────────

func handlePortfolio(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	if len(args) == 0 {
		showPortfolio(ctx, b, chatID)
		return
	}
	switch strings.ToLower(args[0]) {
	case "add":
		if len(args) < 3 {
			send(ctx, b, chatID, "Usage: /portfolio add <symbol> <amount>\nExample: /portfolio add BTC 0.5")
			return
		}
		portfolioAdd(ctx, b, chatID, args[1], args[2])
	case "remove":
		if len(args) < 2 {
			send(ctx, b, chatID, "Usage: /portfolio remove <symbol>")
			return
		}
		portfolioRemove(ctx, b, chatID, args[1])
	default:
		send(ctx, b, chatID, "Usage: /portfolio | /portfolio add <symbol> <amount> | /portfolio remove <symbol>")
	}
}

func showPortfolio(ctx context.Context, b *bot.Bot, chatID int64) {
	mu.RLock()
	entries := make([]PortfolioEntry, len(portfolios[chatID]))
	copy(entries, portfolios[chatID])
	mu.RUnlock()

	if len(entries) == 0 {
		send(ctx, b, chatID, "💼 Your portfolio is empty.\n\nAdd holdings with /portfolio add <symbol> <amount>")
		return
	}

	send(ctx, b, chatID, "💼 Calculating portfolio value...")

	var sb strings.Builder
	sb.WriteString("💼 *Your Portfolio*\n\n")
	total := 0.0
	for _, e := range entries {
		pd, err := fetchPrice(e.CoinID)
		if err != nil {
			sb.WriteString(fmt.Sprintf("• *%s*: %.6f (price unavailable)\n", e.Symbol, e.Amount))
			continue
		}
		val := e.Amount * pd.USD
		total += val
		sb.WriteString(fmt.Sprintf(
			"• *%s*: %.6f × %s = *%s* (%s%.2f%%)\n",
			e.Symbol, e.Amount, fmtUSD(pd.USD), fmtUSD(val),
			signStr(pd.USD24hChange), pd.USD24hChange,
		))
	}
	sb.WriteString(fmt.Sprintf("\n💰 *Total: %s*", fmtUSD(total)))
	send(ctx, b, chatID, sb.String())
}

func portfolioAdd(ctx context.Context, b *bot.Bot, chatID int64, coinInput, amtStr string) {
	coinID, symbol, err := resolveCoinID(coinInput)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	amt, err := strconv.ParseFloat(strings.ReplaceAll(amtStr, ",", ""), 64)
	if err != nil || amt <= 0 {
		send(ctx, b, chatID, "❌ Invalid amount.")
		return
	}
	mu.Lock()
	portfolio := portfolios[chatID]
	found := false
	for i, e := range portfolio {
		if e.Symbol == symbol {
			portfolio[i].Amount += amt
			found = true
			break
		}
	}
	if !found {
		portfolio = append(portfolio, PortfolioEntry{CoinID: coinID, Symbol: symbol, Amount: amt})
	}
	portfolios[chatID] = portfolio
	mu.Unlock()
	send(ctx, b, chatID, fmt.Sprintf("✅ Added *%.6f %s* to your portfolio.", amt, symbol))
}

func portfolioRemove(ctx context.Context, b *bot.Bot, chatID int64, coinInput string) {
	_, symbol, err := resolveCoinID(coinInput)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	mu.Lock()
	var next []PortfolioEntry
	removed := false
	for _, e := range portfolios[chatID] {
		if e.Symbol == symbol {
			removed = true
		} else {
			next = append(next, e)
		}
	}
	portfolios[chatID] = next
	mu.Unlock()

	if removed {
		send(ctx, b, chatID, fmt.Sprintf("✅ Removed *%s* from your portfolio.", symbol))
	} else {
		send(ctx, b, chatID, fmt.Sprintf("❌ *%s* not found in your portfolio.", symbol))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Command: /convert <amount> <from> to <to>
// ─────────────────────────────────────────────────────────────────────────────

func handleConvert(ctx context.Context, b *bot.Bot, chatID int64, args []string) {
	// Accepts: /convert 1 BTC to ETH  OR  /convert 1 BTC ETH
	if len(args) < 3 {
		send(ctx, b, chatID, "Usage: /convert <amount> <from> to <to>\nExample: /convert 1 BTC to ETH")
		return
	}
	amt, err := strconv.ParseFloat(strings.ReplaceAll(args[0], ",", ""), 64)
	if err != nil || amt <= 0 {
		send(ctx, b, chatID, "❌ Invalid amount.")
		return
	}
	fromInput := args[1]
	toInput := args[len(args)-1] // last word, handles "to" keyword between

	fromID, fromSym, err := resolveCoinID(fromInput)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	fromPD, err := fetchPrice(fromID)
	if err != nil {
		send(ctx, b, chatID, "❌ Failed to fetch price for "+fromSym)
		return
	}

	toUpper := strings.ToUpper(strings.TrimSpace(toInput))
	// Treat USD / USDT / USDC / BUSD as stable dollar
	if toUpper == "USD" || toUpper == "USDT" || toUpper == "USDC" || toUpper == "BUSD" {
		result := amt * fromPD.USD
		send(ctx, b, chatID, fmt.Sprintf("💱 *%g %s* = *%s*", amt, fromSym, fmtUSD(result)))
		return
	}

	toID, toSym, err := resolveCoinID(toInput)
	if err != nil {
		send(ctx, b, chatID, "❌ "+err.Error())
		return
	}
	toPD, err := fetchPrice(toID)
	if err != nil {
		send(ctx, b, chatID, "❌ Failed to fetch price for "+toSym)
		return
	}

	result := (amt * fromPD.USD) / toPD.USD
	send(ctx, b, chatID, fmt.Sprintf(
		"💱 *%g %s* = *%.6f %s*\n\n_%s → $%s → %s_",
		amt, fromSym,
		result, toSym,
		fromSym, fmt.Sprintf("%.2f", amt*fromPD.USD), toSym,
	))
}

// ─────────────────────────────────────────────────────────────────────────────
// Background alert checker
// ─────────────────────────────────────────────────────────────────────────────

func alertChecker(ctx context.Context, b *bot.Bot) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	log.Println("🔔 Alert checker started (interval: 60s)")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkAlerts(ctx, b)
		}
	}
}

func checkAlerts(ctx context.Context, b *bot.Bot) {
	mu.RLock()
	if len(alerts) == 0 {
		mu.RUnlock()
		return
	}
	coinSet := make(map[string]bool)
	for _, a := range alerts {
		coinSet[a.CoinID] = true
	}
	mu.RUnlock()

	// Collect unique IDs and fetch prices
	ids := make([]string, 0, len(coinSet))
	for id := range coinSet {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	priceMap := make(map[string]float64)
	for _, id := range ids {
		if pd, err := fetchPrice(id); err == nil {
			priceMap[id] = pd.USD
		}
	}

	mu.Lock()
	var remaining []Alert
	for _, a := range alerts {
		price, ok := priceMap[a.CoinID]
		if !ok {
			remaining = append(remaining, a)
			continue
		}
		triggered := (a.Condition == "above" && price > a.Target) ||
			(a.Condition == "below" && price < a.Target)

		if triggered {
			// Fire notification in a goroutine so we don't block the lock long
			alert := a
			currentPrice := price
			go func() {
				var icon string
				if alert.Condition == "above" {
					icon = "🔼"
				} else {
					icon = "🔽"
				}
				send(ctx, b, alert.ChatID, fmt.Sprintf(
					"🚨 *Price Alert!*\n\n*%s* is now *%s*\n%s %s target of *%s*",
					alert.Symbol, fmtUSD(currentPrice),
					icon, alert.Condition, fmtUSD(alert.Target),
				))
			}()
		} else {
			remaining = append(remaining, a)
		}
	}
	alerts = remaining
	mu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────────────
// .env loader
// ─────────────────────────────────────────────────────────────────────────────

func loadEnv(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
	return scanner.Err()
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	if err := loadEnv(".env"); err != nil {
		log.Printf("Warning: could not load .env: %v", err)
	}

	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("BOT_TOKEN is not set. Add it to .env")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	b, err := bot.New(token, bot.WithDefaultHandler(defaultHandler))
	if err != nil {
		log.Fatalf("Failed to create bot: %v", err)
	}

	go alertChecker(ctx, b)

	log.Println("🚀 CryptoBot is live. Press Ctrl+C to stop.")
	b.Start(ctx)
	log.Println("👋 CryptoBot stopped.")
}
