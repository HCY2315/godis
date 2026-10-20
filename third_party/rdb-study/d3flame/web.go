package d3flame

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
)

// flameTmplData 是页面模板所需的静态资源（CSS/JS），
// 它们都被内嵌在 template.go 里，因此生成的网页可以离线打开、无需外网 CDN。
// 用 template.CSS/template.JS 类型是为了让 html/template 不对这些内容做转义。
var flameTmplData = &struct {
	D3Css        template.CSS
	D3Js         template.JS
	D3Flame      template.JS
	D3Tip        template.JS
	BootstrapCSS template.CSS
}{
	D3Css:        template.CSS(d3Css),
	D3Js:         template.JS(d3Js),
	D3Flame:      template.JS(d3FlameGraphJs),
	D3Tip:        template.JS(d3TipJs),
	BootstrapCSS: template.CSS(bootstrapCSS),
}

// flamegraph 是 /flamegraph 路由的处理函数：渲染火焰图页面。
func flamegraph(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.New("flamegraph").Parse(html))
	err := tmpl.Execute(w, flameTmplData)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("500 - Internal Error"))
	}
}

// FlameItem is an Element in flamegraph
//
// FlameItem 是火焰图上的一个「方块」（树节点）：
//   - Name  节点名（如 "db:0"、"User:"、"others"）
//   - Value 该节点的值/大小（决定方块宽度）
//   - Children 子节点
//
// JSON 字段名被缩短成 n/v/c，是为了减小传给前端的 JSON 体积。
type FlameItem struct {
	Name     string   `json:"n"`
	Value    int      `json:"v"`
	Children children `json:"c,omitempty"`
}

// MarshalJSON 把 children（本质是 map）序列化成「数组」。
// 因为前端 d3-flame-graph 需要的是子节点数组，而不是对象。
func (ch children) MarshalJSON() ([]byte, error) {
	list := make([]*FlameItem, 0, len(ch))
	for _, v := range ch {
		list = append(list, v)
	}
	return json.Marshal(list)
}

// AddChild 向当前节点添加一个子节点（以 Name 为键）。
func (node *FlameItem) AddChild(n *FlameItem) {
	node.Children[n.Name] = n
}

// children 用 map 存子节点，便于按名字快速查找/去重；
// 序列化时再通过上面的 MarshalJSON 转成数组。
type children map[string]*FlameItem

// Web 启动一个 HTTP 服务来展示火焰图，返回一个用于停止服务的 channel。
//
// 路由：
//
//	/flamegraph   渲染页面（内嵌 d3 + d3-flame-graph）
//	/stacks.json  返回火焰图数据（即传入的 data）
//
// 用法：
//
//	stop := d3flame.Web(jsonData, 16379)
//	// 浏览器打开 http://localhost:16379/flamegraph
//	stop <- struct{}{} // 用完关闭
func Web(data []byte, port int) chan<- struct{} {
	mux := http.NewServeMux()
	mux.HandleFunc("/flamegraph", flamegraph)
	mux.HandleFunc("/stacks.json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: mux,
	}
	fmt.Printf("see http://localhost:%d/flamegraph\n", port)
	stop := make(chan struct{})
	go func() {
		<-stop
		_ = server.Close()
	}()
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return stop
}
