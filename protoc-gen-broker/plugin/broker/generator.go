package broker

import (
	"path"
	"strconv"
	"strings"

	pb "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/poonman/shopee/protoc-gen-broker/api"
	"github.com/poonman/shopee/protoc-gen-broker/generator"
)

// Paths for packages used by code generated in this file,
// relative to the import_prefix of the generator.Generator.
const (
	kafkaPkgPath  = "git.garena.com/shopee/pricing/video-crawler/internal/pkg/kafka"
	logPkgPath = "git.garena.com/shopee/pricing/video-crawler/internal/pkg/log"
	rpoolPkgPath  = "git.garena.com/shopee/pricing/video-crawler/internal/pkg/routine_pool"
	codecPkgPath  = "git.garena.com/shopee/pricing/video-crawler/pkg/doom/codec"
	consumerPkgPath  = "git.garena.com/shopee/pricing/video-crawler/pkg/doom/consumer"
	helperPkgPath = "git.garena.com/shopee/pricing/video-crawler/pkg/doom/helper"
)

func init() {
	generator.RegisterPlugin(new(brokerPlugin))
}

// brokerPlugin is an implementation of the Go protocol buffer compiler's
// plugin architecture.  It generates bindings for go-kitPlugin support.
type brokerPlugin struct {
	gen *generator.Generator
}

// Name returns the name of this plugin, "kitPlugin".
func (g *brokerPlugin) Name() string {
	return "broker"
}

// The names for packages imported in the generated code.
// They may vary from the final path component of the import path
// if the name is used by other packages.
var (
	kafkaPkg  string
	logPkg string
	rpoolPkg  string
	codecPkg  string
	consumerPkg string
	helperPkg string
	pkgImports map[generator.GoPackageName]bool
)

// Init initializes the plugin.
func (g *brokerPlugin) Init(gen *generator.Generator) {
	g.gen = gen
	kafkaPkg = generator.RegisterUniquePackageName("kafka", nil)
	logPkg = generator.RegisterUniquePackageName("log", nil)
	rpoolPkg = generator.RegisterUniquePackageName("routine_pool", nil)
	codecPkg = generator.RegisterUniquePackageName("codec", nil)
	consumerPkg = generator.RegisterUniquePackageName("consumer", nil)
	helperPkg = generator.RegisterUniquePackageName("helper", nil)
}

// Given a type name defined in a .proto, return its object.
// Also record that we're using it, to guarantee the associated import.
func (g *brokerPlugin) objectNamed(name string) generator.Object {
	g.gen.RecordTypeUse(name)
	return g.gen.ObjectNamed(name)
}

// Given a type name defined in a .proto, return its name as we will print it.
func (g *brokerPlugin) typeName(str string) string {
	return g.gen.TypeName(g.objectNamed(str))
}

// P forwards to g.gen.P.
func (g *brokerPlugin) P(args ...interface{}) { g.gen.P(args...) }

// Generate generates code for the services in the given file.
func (g *brokerPlugin) Generate(file *generator.FileDescriptor) {
	if len(file.FileDescriptorProto.Service) == 0 {
		return
	}
	//g.P("// Reference imports to suppress errors if they are not otherwise used.")
	//g.P("var _ ", statusPkg, ".Endpoint")
	//g.P("var _ ", contextPkg, ".Context")
	//g.P("var _ ", clientPkg, ".Option")
	//g.P("var _ ", serverPkg, ".Option")
	//g.P()

	for i, service := range file.FileDescriptorProto.Service {
		g.generateService(file, service, i)
	}
}

// GenerateImports generates the import declaration for this file.
func (g *brokerPlugin) GenerateImports(file *generator.FileDescriptor, imports map[generator.GoImportPath]generator.GoPackageName) {
	if len(file.FileDescriptorProto.Service) == 0 {
		return
	}
	g.P("import (")
	g.P(kafkaPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, kafkaPkgPath)))
	g.P(logPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, logPkgPath)))
	g.P(rpoolPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, rpoolPkgPath)))
	g.P(codecPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, codecPkgPath)))
	g.P(consumerPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, consumerPkgPath)))
	g.P(helperPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, helperPkgPath)))
	g.P(")")
	g.P()

	// We need to keep track of imported packages to make sure we don't produce
	// a name collision when generating types.
	pkgImports = make(map[generator.GoPackageName]bool)
	for _, name := range imports {
		pkgImports[name] = true
	}
}

// reservedClientName records whether a client name is reserved on the client side.
var reservedClientName = map[string]bool{
	// TODO: do we need any in go-kitPlugin?
}

func unexport(s string) string {
	if len(s) == 0 {
		return ""
	}
	name := strings.ToLower(s[:1]) + s[1:]
	if pkgImports[generator.GoPackageName(name)] {
		return name + "_"
	}
	return name
}

func (g *brokerPlugin) generateService(file *generator.FileDescriptor, service *pb.ServiceDescriptorProto, index int) {
	serviceRule := GetServiceRule(service)
	if serviceRule == nil {
		return
	}

	switch serviceRule.GetPattern().(type) {
	case *api.ServiceRule_Broker:
		g.generateKafkaBrokerService(file, service)
	default:
		return
	}
}

func (g *brokerPlugin) generateKafkaBrokerService(file *generator.FileDescriptor, service *pb.ServiceDescriptorProto) {
	origServName := service.GetName()
	svcName := generator.CamelCase(origServName)
	//unexportSvcName := unexport(svcName)

	// type XXXConsumer struct
	{
		g.P(`type `,svcName,`Consumer struct {`)
		g.P(`kafkaConsumer *consumer.Consumer`)
		g.P(`rpool         *routine_pool.RoutinePool`)
		g.P(`ctx           context.Context`)
		g.P(`cancel        context.CancelFunc`)
		g.P(`topicSuffix   string`)
		g.P(`codec         codec.Codec`)
		g.P(`mu            sync.RWMutex`)
		g.P(`routes        map[string]func([]byte) error`)
		g.P(`}`)
		g.P()
	}
	// type HandleXXX func(ctx context.Context, msg *common.Command) (err error)
	{
		for _, method := range service.GetMethod() {
			inType := g.typeName(method.GetInputType())
			g.P(`type HandleCommand func(msg *`,inType,`) (err error)`)
		}
	}
	{
		g.P(`func New`,svcName,`Consumer(concurrency int, contentType string) *`,svcName,`Consumer {`)
		g.P(`c := &`,svcName,`Consumer{}`)
		g.P(`c.codec = helper.GetCodec(contentType)`)
		g.P(`c.rpool = routine_pool.NewRoutinePool(concurrency)`)
		g.P(`c.routes = make(map[string]func([]byte) error`,strconv.FormatInt(int64(len(service.GetMethod())), 10),`)`)
		g.P(`c.ctx, c.cancel = context.WithCancel(context.TODO())`)
		g.P(`return c`)
		g.P(`}`)
		g.P()
	}
	{
		g.P(`type `,svcName,`Option func(consumer *`,svcName,`Consumer)`)
		g.P()
	}
	{
		for _, method := range service.GetMethod() {
			inType := g.typeName(method.GetInputType())
			g.P(`func Subscribe`,svcName,method.GetName(),`(handle Handle`,method.GetName(),`) `,svcName,`Option {`)
			g.P(`return func(c *`,svcName,`Consumer) {`)
			g.P(`c.routes["Command"+c.topicSuffix] = func(data []byte) error {`)
			g.P(`msg := &`,inType,`{}`)
			g.P(`if err := c.codec.Unmarshal(data, msg); err != nil {`)
			g.P(`return err`)
			g.P(`}`)
			g.P(`return handle(msg)`)
			g.P(`}`)
			g.P(`}`)
			g.P(`}`)
			g.P()
		}
	}

	{
		g.P(`func (c *`,svcName,`Consumer) Serve(brokers []string, group, topicSuffix string, opts ...`,svcName,`Option) error {`)
		g.P(`c.topicSuffix = topicSuffix`)
		g.P(`for _, opt := range opts {`)
		g.P(`opt(c)`)
		g.P(`}`)
		g.P(`topics := make([]string, 0, len(c.routes))`)
		g.P(`for topic := range c.routes {`)
		g.P(`topics = append(topics, topic)`)
		g.P(`}`)
		g.P(`kafkaConsumer, err := consumer.NewConsumer(brokers, topics, group)`)
		g.P(`if err != nil {`)
		g.P(`return err`)
		g.P(`}`)
		g.P(`c.kafkaConsumer = kafkaConsumer`)
		g.P()
		g.P(`Loop:`)
		g.P(`for {`)
		g.P(`select {`)
		g.P(`case <-c.ctx.Done():`)
		g.P(`break Loop`)
		g.P(`default:`)
		g.P(`}`)
		g.P(`msg, err := c.kafkaConsumer.Consume(c.ctx)`)
		g.P(`if err != nil {`)
		g.P(`log.Errorf("failed to consume one command: %s", err)`)
		g.P(`continue`)
		g.P(`}`)
		g.P(`c.mu.RLock()`)
		g.P(`h, ok := c.routes[msg.Topic]`)
		g.P(`c.mu.RUnlock()`)
		g.P(`if ok {`)
		g.P(`c.rpool.Add(func() {`)
		g.P(`_ = h(msg.Data)`)
		g.P(`})`)
		g.P(`}`)
		g.P(`}`)
		g.P(`return nil`)
		g.P(`}`)
		g.P()
	}
	{
		g.P(`func (c *`,svcName,`Consumer) Close() {`)
		g.P(`c.cancel()`)
		g.P(`_ = c.kafkaConsumer.Close()`)
		g.P(`}`)
		g.P()
	}
	{
		g.P(`type `,svcName,`Producer struct {`)
		g.P(`producer     *kafka.SyncProducer`)
		g.P(`topicSuffix  string`)
		g.P(`topicCommand string`)
		g.P(`codec        codec.Codec`)
		g.P(`}`)
		g.P()
	}
	{
		g.P(`func New`,svcName,`Producer(brokers []string, topicSuffix string) *`,svcName,`Producer {`)
		g.P(`p := &`,svcName,`Producer{`)
		g.P(`producer:     kafka.NewSyncProducer(brokers),`)
		g.P(`topicSuffix:  topicSuffix,`)
		g.P(`topicCommand: "Command" + topicSuffix,`)
		g.P(`codec:        helper.GetCodec(contentType),`)
		g.P(`}`)
		g.P(`return p`)
		g.P(`}`)
		g.P()
	}
	{
		for _, method := range service.GetMethod() {
			inType := g.typeName(method.GetInputType())
			g.P(`func (p *`,svcName,`Producer) Publish`,method.GetName(),`(msg *`,inType,`) error {`)
			g.P(`data, err := p.codec.Marshal(msg)`)
			g.P(`if err != nil {`)
			g.P(`return err`)
			g.P(`}`)
			g.P(`return p.producer.Produce(p.topic`,method.GetName(),`, data)`)
			g.P(`}`)
			g.P()
		}
	}
	{
		g.P(`func (p *`,svcName,`Producer) Close() error {`)
		g.P(`return p.producer.Close()`)
		g.P(`}`)
	}
}


