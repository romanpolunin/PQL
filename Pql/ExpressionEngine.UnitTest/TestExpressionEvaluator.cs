using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Irony.Parsing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.ExpressionEngine.Compiler;
using Pql.ExpressionEngine.Interfaces;

namespace Pql.ExpressionEngine.UnitTest
{
    [TestClass]
    public class TestExpressionEvaluator
    {
        ExpressionEvaluatorRuntime m_runtime;

        [TestInitialize]
        public void TestInitialize()
        {
            m_runtime = new ExpressionEvaluatorRuntime();
        }

        private static long s_elapsedMilliseconds;
        private static long s_totalOperations;

        [TestMethod]
        //[Ignore]
        public void TestDebug()
        {
            var s1 = "по-русски";
            var s2 = "По-Русски";
            Assert.IsTrue(0 == s1.IndexOf(s2, StringComparison.OrdinalIgnoreCase));

            //var processor = m_runtime.Compile<object, bool>("Contains('abcd', 'bc')");
            var processor = m_runtime.Compile<float, bool>("case case (@context) WHEN -1 then true WHEN (cast(1+0.5,'Single')) THEN true END WHEN true THEN false ELSE true end");
            //var processor = m_runtime.Compile<bool>("EndsWith('abcd', 'cd')");
            Assert.IsTrue(processor(1.4f));

            var numPerThread = 10000;
            var numThreads = 4;
            var start = new ManualResetEventSlim(false);
            var stop = new Semaphore(0, numThreads);

            s_elapsedMilliseconds = 0;
            s_totalOperations = 0;

            Action test = () =>
                {
                    start.Wait();

                    var watch = Stopwatch.StartNew();
                    for (var i = 0; i < numPerThread; i++)
                    {
                        m_runtime.Compile<float, bool>("case case (@context) WHEN -1 then true WHEN (cast(1+0.5,'Single')) THEN true END WHEN true THEN false ELSE true end")(1.4f);
                    }
                    watch.Stop();

                    Interlocked.Add(ref s_elapsedMilliseconds, watch.ElapsedMilliseconds);
                    Interlocked.Add(ref s_totalOperations, numPerThread);

                    stop.Release();
                };

            var threads = new Thread[numThreads];
            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(new ThreadStart(test));
                threads[i].Start();
            }

            Thread.Sleep(1000);
            start.Set();

            for (var i = 0; i < numThreads; i++)
            {
                stop.WaitOne();
            }

            Console.WriteLine(s_elapsedMilliseconds);
            Console.WriteLine(s_totalOperations);
            Console.WriteLine(((double) s_totalOperations) * 1000 / s_elapsedMilliseconds);
        }

        [TestMethod]
        public void TestDateTimesAndTimeSpans()
        {
            var december132013 = new DateTime(2013, 12, 13);
            var addOneDay = TimeSpan.FromDays(1);
            var addOneHour = TimeSpan.FromHours(1);
            var subtractOneDay = TimeSpan.FromDays(-1);
            var formatString = "yyyy-MM-dd-HH-mm-ss";
            var valueString = december132013.AddHours(13).AddMinutes(35).AddSeconds(4).ToString(formatString);

            Assert.AreEqual(december132013, Eval<string, DateTime>("convert(@context, 'DateTime')", "12-13-2013"));
            Assert.AreEqual(addOneDay, Eval<string, TimeSpan>("convert(@context, 'TimeSpan')", "1.00:00:00"));
            Assert.AreEqual(subtractOneDay, Eval<string, TimeSpan>("convert(@context, 'TimeSpan')", "-1.00:00:00"));

            Assert.IsTrue(december132013.AddDays(-1) <= Convert.ToDateTime("12-13-2013"));

            var eval = m_runtime.Compile<DateTime, bool>("@context <= convert('12-13-2013', 'DateTime')");
            Assert.IsTrue(eval(december132013));
            Assert.IsTrue(eval(december132013.AddDays(-1)));
            Assert.IsFalse(eval(december132013.AddHours(1)));

            eval = m_runtime.Compile<DateTime, bool>("@context <= convert('13-Dec-2013', 'DateTime')");
            Assert.IsTrue(eval(december132013));
            Assert.IsTrue(eval(december132013.AddDays(-1)));
            Assert.IsFalse(eval(december132013.AddHours(1)));

            var eval2 = m_runtime.Compile<DateTime, TimeSpan>("@context - convert('13-Dec-2013', 'DateTime')");
            Assert.AreEqual(addOneDay, eval2(december132013.AddDays(1)));
            
            var eval3 = m_runtime.Compile<DateTime, DateTime>("@context + convert('01:00:00', 'TimeSpan')");
            Assert.AreEqual(december132013 + addOneHour, eval3(december132013));

            eval3 = m_runtime.Compile<DateTime, DateTime>("convert('01:00:00', 'TimeSpan') + @context");
            Assert.AreEqual(december132013 + addOneHour, eval3(december132013));

            var eval4 = m_runtime.Compile<TimeSpan, TimeSpan>("@context + convert('01:00:00', 'TimeSpan')");
            Assert.AreEqual(addOneHour + addOneHour, eval4(addOneHour));

            var eval5 = m_runtime.Compile<object, DateTime>("ToDateTime(2001,10,1,12,13,14)");
            Assert.AreEqual(new DateTime(2001,10,1,12,13,14), eval5(null));

            var date2 = new DateTime(2001, 10, 1, 12, 13, 14);
            var eval6 = (Func<int, int, int, int, int, int, DateTime>) m_runtime.Compile("ToDateTime(@y,@m,@d,@hh,@mm,@ss)", typeof(DateTime),
                new Tuple<string, Type>("@y", typeof(int)),
                new Tuple<string, Type>("@m", typeof(int)),
                new Tuple<string, Type>("@d", typeof(int)),
                new Tuple<string, Type>("@hh", typeof(int)),
                new Tuple<string, Type>("@mm", typeof(int)),
                new Tuple<string, Type>("@ss", typeof(int))
                );
            Assert.AreEqual(date2, eval6(date2.Year, date2.Month, date2.Day, date2.Hour, date2.Minute, date2.Second));

            var date2Dateonly = new DateTime(2001, 10, 1);
            var eval6DateOnly = (Func<int, int, int, DateTime>) m_runtime.Compile("ToDateTime(@y,@m,@d)", typeof(DateTime),
                new Tuple<string, Type>("@y", typeof(int)),
                new Tuple<string, Type>("@m", typeof(int)),
                new Tuple<string, Type>("@d", typeof(int))
                );
            Assert.AreEqual(date2Dateonly, eval6DateOnly(date2.Year, date2.Month, date2.Day));
            Assert.AreEqual(0, eval6DateOnly(date2.Year, date2.Month, date2.Day).Hour);
            Assert.AreEqual(0, eval6DateOnly(date2.Year, date2.Month, date2.Day).Minute);
            Assert.AreEqual(0, eval6DateOnly(date2.Year, date2.Month, date2.Day).Second);
            Assert.AreEqual(0, eval6DateOnly(date2.Year, date2.Month, date2.Day).Millisecond);
            Assert.AreEqual(DateTimeKind.Unspecified, eval6DateOnly(date2.Year, date2.Month, date2.Day).Kind);

            var date3 = new DateTime(2001, 10, 1, 12, 13, 14, 123);
            var eval6Withms = (Func<int, int, int, int, int, int, int, DateTime>) m_runtime.Compile("ToDateTime(@y,@m,@d,@hh,@mm,@ss,@millis)", typeof(DateTime),
                new Tuple<string, Type>("@y", typeof(int)),
                new Tuple<string, Type>("@m", typeof(int)),
                new Tuple<string, Type>("@d", typeof(int)),
                new Tuple<string, Type>("@hh", typeof(int)),
                new Tuple<string, Type>("@mm", typeof(int)),
                new Tuple<string, Type>("@ss", typeof(int)),
                new Tuple<string, Type>("@millis", typeof(int))
                );
            Assert.AreEqual(date3, eval6Withms(date2.Year, date3.Month, date3.Day, date3.Hour, date3.Minute, date3.Second, date3.Millisecond));

            var eval7 = (Func<string, string, DateTime>) m_runtime.Compile(
                "ToDateTime(@value, @format)", typeof (DateTime),
                new Tuple<string, Type>("@value", typeof (string)),
                new Tuple<string, Type>("@format", typeof (string)));
            Assert.AreEqual(DateTime.ParseExact(valueString, formatString, null), eval7(valueString, formatString));

            var eval8 = (Func<DateTime>) m_runtime.Compile(
                string.Format("ToDateTime('{0}', '{1}')", valueString, formatString),
                typeof (DateTime));
            Assert.AreEqual(DateTime.ParseExact(valueString, formatString, null), eval8());

            Assert.AreEqual(date2, Eval<Int64, DateTime>("ToDateTime(" + date2.Ticks + ")", 0));
            Assert.AreEqual(date2, Eval<Int64, DateTime>("ToDateTime(@context)", date2.Ticks));
            Assert.AreEqual(date2, Eval<UnboxableNullable<Int64>, DateTime>("ToDateTime(@context)", date2.Ticks));
        }

        [TestMethod]
        public void TestBetween()
        {
            var evalInt = m_runtime.Compile<int, bool>("@context between 1 and 5");
            Assert.IsTrue(evalInt(1));
            Assert.IsTrue(evalInt(2));
            Assert.IsTrue(evalInt(5));
            Assert.IsFalse(evalInt(6));
            Assert.IsFalse(evalInt(0));

            evalInt = m_runtime.Compile<int, bool>("@context not between 1 and 5");
            Assert.IsTrue(evalInt(-1));
            Assert.IsTrue(evalInt(0));
            Assert.IsTrue(evalInt(6));
            Assert.IsFalse(evalInt(2));
            Assert.IsFalse(evalInt(5));

            //yes, this one is incorrect on purpose
            evalInt = m_runtime.Compile<int, bool>("@context between 5 and 1"); 
            Assert.IsFalse(evalInt(0));
            Assert.IsFalse(evalInt(1));
            Assert.IsFalse(evalInt(2));
            Assert.IsFalse(evalInt(3));
            Assert.IsFalse(evalInt(4));
            Assert.IsFalse(evalInt(5));
            Assert.IsFalse(evalInt(6));

            var evalDouble = m_runtime.Compile<Double, bool>("@context + 1 not between 1.0 and 5.0");
            Assert.IsTrue(evalDouble(-2));
            Assert.IsTrue(evalDouble(-.01));
            Assert.IsTrue(evalDouble(4.001));
            Assert.IsFalse(evalDouble(0.0));
            Assert.IsFalse(evalDouble(4.0));
            Assert.IsFalse(evalDouble(3.99));

            var evalString = m_runtime.Compile<string, bool>("@context between 'abc' and 'ABH'");
            Assert.IsTrue(evalString("abc"));
            Assert.IsTrue(evalString("ABC"));
            Assert.IsTrue(evalString("abd"));
            Assert.IsTrue(evalString("abD"));
            Assert.IsTrue(evalString("ABH"));
            Assert.IsTrue(evalString("ABh"));
            Assert.IsFalse(evalString("abb"));
            Assert.IsFalse(evalString("abB"));
            Assert.IsFalse(evalString("abj"));
            Assert.IsFalse(evalString("abJ"));

            var evalDate = m_runtime.Compile<DateTime, bool>("@context between convert('Dec-1-2000', 'DateTime') and convert('Dec-1-2001', 'DateTime')");
            var dec12000 = new DateTime(2000, 12, 1);
            Assert.IsFalse(evalDate(dec12000.AddDays(-1)));
            Assert.IsTrue(evalDate(dec12000));
            Assert.IsTrue(evalDate(dec12000.AddDays(1)));
            Assert.IsTrue(evalDate(dec12000.AddYears(1)));
            Assert.IsFalse(evalDate(dec12000.AddYears(1).AddSeconds(1)));

            var evalTimeSpan = m_runtime.Compile<TimeSpan, bool>("@context between convert('00:00:05', 'TimeSpan') and convert('01:59:06', 'TimeSpan')");
            Assert.IsFalse(evalTimeSpan(TimeSpan.FromSeconds(4) + TimeSpan.FromMilliseconds(999)));
            Assert.IsTrue(evalTimeSpan(TimeSpan.FromSeconds(4) + TimeSpan.FromMilliseconds(1000)));
            Assert.IsTrue(evalTimeSpan(TimeSpan.FromHours(1)));
            Assert.IsTrue(evalTimeSpan(TimeSpan.FromHours(1) + TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(6)));
            Assert.IsFalse(evalTimeSpan(TimeSpan.FromHours(1) + TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(7)));
        }

        [TestMethod]
        public void TestSpecialCharactersInLiterals()
        {
            Assert.AreEqual("hello\nworld".Split('\n').Length, Eval<string>(@"'hello\nworld'").Split('\n').Length);
            Assert.AreEqual("hello\rworld".Split('\r').Length, Eval<string>(@"'hello\rworld'").Split('\r').Length);

            Assert.AreEqual(
                "hello\r\nworld".Split(new[] {"\r\n"}, StringSplitOptions.None).Length,
                Eval<string>(@"'hello\r\nworld'").Split(new[] { "\r\n" }, StringSplitOptions.None).Length);
            
            Assert.AreEqual(
                "hello\r\nworld".Split(new[] {"\r", "\n"}, StringSplitOptions.None).Length,
                Eval<string>(@"'hello\r\nworld'").Split(new[] { "\r", "\n" }, StringSplitOptions.None).Length);

            Assert.AreEqual(
                "h\r\nello\tworld".Split('\t', '\r', '\n').Length,
                Eval<string>("'h\r\nello\tworld'").Split('\t', '\r', '\n').Length);

            Assert.AreEqual(-10, Eval<int>("-10"));
            Assert.AreEqual(-1.5e6, Eval<double>("-1.5e6"));
            Assert.AreEqual(-1.5e-6, Eval<double>("-1.5e-6"));
            Assert.AreEqual(-0.5e-6, Eval<double>("-.5e-6"));
            Assert.AreEqual(-5.0e-6, Eval<double>("-5.e-6"));
            Assert.AreEqual(-5.0/3, Eval<double>("-5./3"));
            Assert.AreEqual(-5 / 3, Eval<int>("-5/3"));
        }

        [TestMethod]
        public void TestCaseStatement()
        {
            Assert.AreEqual(4, Eval<Int32>("case when 1=2 then 3 when 2=3 then 5 else 4 end"));
            Assert.AreEqual(4, Eval<Int32>("case when 1+2=3 then 4 end"));
            Assert.AreEqual(1.5, Eval<Double>("case when 1+2=3 then 1.5 else 4 end"));
            Assert.AreEqual(1.5, Eval<Double>("case when 1+2>3 then 4 else 1.5 end"));
            Assert.AreEqual(1.5, Eval<Double>("case when 1+2=3 then 1.5 else convert('4', 'double') end"));
            
            Assert.AreEqual(3.0, m_runtime.Compile<Double, Double>("case @context when 2.5,3  then 1.5 else 3 end")(1.5));
            Assert.AreEqual(3.0, m_runtime.Compile<Double, Double>("case @context when 4,   2.5 then 1.5 else 3   end")(3));
            Assert.AreEqual(3.0, m_runtime.Compile<Double, Double>("case @context when 2.5, 4   then 1.5 else 3   end")(3));
            Assert.AreEqual(1.5, m_runtime.Compile<Double, Double>("case @context when 4,   2.5 then cast(3, 'single') else 1.5 end")(3));
            Assert.AreEqual(1.5, m_runtime.Compile<Double, Double>("case @context when 2.5, 4   then cast(3, 'double') else 1.5 end")(3));

            var eval3 = m_runtime.Compile<bool, bool>("case @context when false then true else false end");
            Assert.IsTrue(eval3(false));
            Assert.IsTrue(Eval<string, bool>("case @context WHEN 'abc' then false WHEN 'cde' then false WHEN 'test' then true ELSE false END", "test"));
            Assert.IsTrue(Eval<string, bool>("case @context WHEN ('a','b','c') then true WHEN ('c','d','e') then false ELSE false END", "a"));
            Assert.IsTrue(Eval<string, bool>("case @context WHEN ('a','b','c') then false WHEN ('c','d','e') then true ELSE false END", "e"));
            Assert.IsTrue(Eval<string, bool>("case @context WHEN 'a','b','c' then true WHEN 'c','d','e' then false ELSE false END", "a"));
            Assert.IsTrue(Eval<string, bool>("case @context WHEN (('a','b','c')) then false WHEN (('c','d','e')) then true ELSE false END", "e"));
            Assert.IsTrue(Eval<double, bool>("case @context WHEN (-1) then false WHEN 1+0.5 then true ELSE false END", 1.5));
            Assert.IsFalse(Eval<bool>("case WHEN case WHEN true THEN (true) ELSE false end THEN false ELSE true END"));
            Assert.IsTrue(Eval<bool, bool>("case case WHEN @context THEN 1.5 ELSE 0 end WHEN 1.5 THEN true ELSE false END", true));
            
            var eval4 = (Func<Single, bool>)m_runtime.Compile("case @arg1 WHEN -1 then false WHEN cast(1+0.5,'Single') then true ELSE false END", 
                typeof(Boolean), new Tuple<string, Type>("@arg1", typeof(Single)));
            Assert.IsTrue(eval4(1.5f));
            Assert.IsFalse(eval4(1.4f));

            eval4 = (Func<Single, bool>)m_runtime.Compile("case @arg1 WHEN -1 then true WHEN cast(1+0.5,'Single') then true END", 
                typeof(Boolean), new Tuple<string, Type>("@arg1", typeof(Single)));
            Assert.IsTrue(eval4(1.5f));
            Assert.IsFalse(eval4(1.4f));
            Assert.IsTrue(eval4(-1f));

            eval4 = (Func<Single, bool>)m_runtime.Compile("case case (@arg1) WHEN -1 then true WHEN (cast(1+0.5,'Single')) THEN true END WHEN true THEN false ELSE true end", 
                typeof(Boolean), new Tuple<string, Type>("@arg1", typeof(Single)));
            Assert.IsFalse(eval4(1.5f));
            Assert.IsTrue(eval4(1.4f));
            Assert.IsFalse(eval4(-1f));
        }

        class SomeItem
        {
            public long EtlAccountId;
            public SomeContainer[] SomeContainerData { get; set; }
        }

        class SomeContainer
        {
            
        }

        [TestMethod]
        public void TestAutoFieldAndPropertyDiscovery()
        {
            var data = new SomeItem();

            var simpleContextEval = m_runtime.Compile<SomeItem, bool>("EtlAccountId = 1");
            data.EtlAccountId = 1;
            Assert.IsTrue(simpleContextEval(data));
            data.EtlAccountId = 0;
            Assert.IsFalse(simpleContextEval(data));

            var nestedEval = m_runtime.Compile<SomeItem, long>("@Context.EtlAccountId");
            data.EtlAccountId = 1;
            Assert.AreEqual(data.EtlAccountId, nestedEval(data));
            data.EtlAccountId = 2;
            Assert.AreEqual(data.EtlAccountId, nestedEval(data));

            var nestedEval2 = m_runtime.Compile<SomeItem, int>("@Context.SomeContainerData.Length");
            data.SomeContainerData = new SomeContainer[0];
            Assert.AreEqual(data.SomeContainerData.Length, nestedEval2(data));
            data.SomeContainerData = new SomeContainer[10];
            Assert.AreEqual(data.SomeContainerData.Length, nestedEval2(data));

            nestedEval2 = m_runtime.Compile<SomeItem, int>("SomeContainerData.Length");
            data.SomeContainerData = new SomeContainer[0];
            Assert.AreEqual(data.SomeContainerData.Length, nestedEval2(data));
            data.SomeContainerData = new SomeContainer[10];
            Assert.AreEqual(data.SomeContainerData.Length, nestedEval2(data));
        }

        [TestMethod]
        public void TestAnalyzerExtensions()
        {
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Function, "CustomEndsWith", (root, state) =>
            {
                var method = PrepareStringInstanceMethodCall("EndsWith", root, state, out var value, out var pattern);

                return Expression.Condition(
                    Expression.ReferenceEqual(Expression.Constant(null), value),
                    Expression.Constant(false),
                    Expression.Condition(
                        Expression.ReferenceEqual(Expression.Constant(null), pattern),
                        Expression.Constant(false),
                        Expression.Call(value, method, pattern, Expression.Constant(StringComparison.OrdinalIgnoreCase))));
            }));

            var eval = (Func<string, string, bool>) m_runtime.Compile(
                "CustomEndsWith(@arg1, @arg2)", typeof (bool), 
                new Tuple<string, Type>("@arg1", typeof (string)),
                new Tuple<string, Type>("@arg2", typeof(string)));

            Assert.IsTrue(eval("abcde", "de"));
            Assert.IsFalse(eval("abcde", "ee"));

            var watch = Stopwatch.StartNew();
            var num = 10000000;
            for (var i = 0; i < num; i++)
            {
                eval("abcde", "de");
            }
            watch.Stop();

            Console.WriteLine(watch.ElapsedMilliseconds + ", " + (double)num * 1000 / watch.ElapsedMilliseconds);

            // now demonstrate implicit access to context information
            m_runtime.RegisterAtom(
                new AtomMetadata(
                    AtomType.Function, "StringValueAt", (root, state) =>
                        {
                            root.RequireChild(null, 1, 0).RequireChildren(1);

                            var arg1Node = root.RequireChild(null, 1, 0, 0);
                            var index = state.ParentRuntime.Analyze(arg1Node, state);
                            index.RequireInteger(arg1Node);

                            var array = Expression.Field(state.Context, "StringValues");
                            Expression value = Expression.ArrayIndex(array, index);
                            
                            return Expression.Condition(
                                Expression.ReferenceEqual(Expression.Constant(null), array),
                                Expression.Constant(null, typeof(string)), value);
                        }));

            var data = new SomeDataObject {StringValues = new[] {"aa", "bb"}};
            Assert.AreEqual("aa", Eval<SomeDataObject, string>("StringValueAt(0)", data));
            Assert.AreEqual("bb", Eval<SomeDataObject, string>("StringValueAt(1)", data));
        }

        [TestMethod]
        public void TestAnalyzerExtensionsAtomHandlers()
        {
            var dateObject = new DateTime();
            var defaultDateObject = dateObject.AddDays(100);
            
            m_runtime.RegisterDynamicAtomHandler(new AtomMetadata(AtomType.Identifier, "RootLevelFields", (root, state) =>
            {
                var name = root.Token.ValueString;
                if (0 == StringComparer.OrdinalIgnoreCase.Compare(name, "root.id.id"))
                {
                    return Expression.Constant(dateObject);
                }

                if (0 == StringComparer.OrdinalIgnoreCase.Compare(name, "isdefault"))
                {
                    return Expression.Constant(defaultDateObject);
                }

                // convention for handlers is to return null, not throw
                return null;
            }));

            var eval = m_runtime.Compile<object, int>("\"root.id.id\".Year");
            Assert.AreEqual(dateObject.Year, eval(null));
            
            eval = m_runtime.Compile<object, int>("isdefault.DayOfYear");
            Assert.AreEqual(defaultDateObject.DayOfYear, eval(null));

            var eval2 = m_runtime.Compile<object, DateTime>("isdefault");
            Assert.AreEqual(dateObject.AddDays(100), eval2(null));
        }

        [TestMethod]
        public void TestRuntimeExtensions()
        {
            Func<string, string, bool> customEndsWith = (s1, s2) => s1 != null && s2 != null && s1.EndsWith(s2, StringComparison.OrdinalIgnoreCase);
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Function, "CustomEndsWith", customEndsWith));

            var eval = (Func<string, string, bool>) m_runtime.Compile(
                "CustomEndsWith(@arg1, @arg2)", typeof (bool), 
                new Tuple<string, Type>("@arg1", typeof (string)),
                new Tuple<string, Type>("@arg2", typeof(string)));

            Assert.IsTrue(eval("abcde", "de"));
            Assert.IsFalse(eval("abcde", "ee"));

            Func<SomeDataObject, int, string> getProperty = (c, ix) => c.StringValues == null ? null : c.StringValues[ix];
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Function, "StringArr", getProperty));

            var data = new SomeDataObject {StringValues = new [] {"aa", "bb"}};
            var result = Eval<SomeDataObject, string>("StringArr(@context, 0)", data);
            Assert.AreEqual(result, "aa");

            Func<string[], int, string> getStringArrayItem = (c, ix) => c == null ? null : c[ix];
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Function, "StringAt", getStringArrayItem));

            Func<SomeDataObject, SomeDataObject> ctx = c => c;
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Identifier, "Contract", ctx));

            result = Eval<SomeDataObject, string>("StringAt(@context.StringValues, 0)", data);
            Assert.AreEqual(result, "aa");

            result = Eval<SomeDataObject, string>("StringAt(Contract.StringValues, 0)", data);
            Assert.AreEqual(result, "aa");
        }

        private MethodInfo PrepareStringInstanceMethodCall(string methodName, ParseTreeNode root, CompilerState state, out Expression value, out Expression pattern)
        {
            root.RequireChildren(2);
            var method = typeof(string).GetMethod(
                methodName, BindingFlags.Instance | BindingFlags.Public, null, new[] { typeof(string), typeof(StringComparison) }, null);

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            value = state.ParentRuntime.Analyze(arg1Node, state);
            value.RequireString(arg1Node);

            var arg2Node = root.RequireChild(null, 1, 0, 1);
            pattern = state.ParentRuntime.Analyze(arg2Node, state);
            pattern.RequireString(arg2Node);
            return method;
        }

        [TestMethod]
        public void TestArgumentTypes()
        {
            var add = (Func<long, long, long>) 
                m_runtime.Compile("@Arg1 + @Arg2",
                typeof(long),
                new Tuple<string, Type>("@arg1", typeof(long)),
                new Tuple<string, Type>("@arg2", typeof(long)));
            
            Assert.AreEqual(1L + 2L, add(1L, 2L));

            var concat = (Func<string, long, string>) 
                m_runtime.Compile("@Arg1 + Convert(@Arg2, 'string')",
                typeof(string),
                new Tuple<string, Type>("@arg1", typeof(string)),
                new Tuple<string, Type>("@arg2", typeof(long)));
            
            Assert.AreEqual("1" + 2L, concat("1", 2L));
        }
        
        [TestMethod]
        public void CheckTypeCasts()
        {
            Assert.AreEqual(Byte.MaxValue, Eval<Byte>("cast(-1, 'Byte')"));
            Assert.AreEqual(1, Eval<Byte>("1"));
            Assert.AreEqual(255, Eval<Byte>("255"));
            Assert.AreEqual(-1, Eval<SByte>("-1"));
            Assert.AreEqual(-1, Eval<SByte>("cast(-1, 'SByte')"));
            Assert.AreEqual(UInt16.MaxValue, Eval<UInt16>("cast(-1, 'UInt16')"));
            Assert.AreEqual(-1, Eval<Int16>("-1"));
            Assert.AreEqual(-1, Eval<Int16>("cast(-1, 'Int16')"));
            Assert.AreEqual(UInt32.MaxValue, Eval<UInt32>("cast(-1, 'UInt32')"));
            Assert.AreEqual(156, Eval<Int32>("convert('156', 'Int32')"));

            var data = new SizableArrayOfByte(new byte[] {0, 1, 2, 255});
            var base64 = Convert.ToBase64String(data.Data, 0, data.Length);
            var newdata = Eval<SizableArrayOfByte>("convert('" + base64 + "', 'binary')");
            Assert.IsNotNull(newdata);
            Assert.IsTrue(data.Data.SequenceEqual(newdata.Data));

            var newstring = Eval<SizableArrayOfByte, string>("convert(@context, 'string')", newdata);
            Assert.AreEqual(base64, newstring);
            newstring = Eval<SizableArrayOfByte, string>("convert(@context, 'string')", null);
            Assert.IsNull(newstring);

            var guid = Eval<Guid>("newguid()");
            Assert.AreNotEqual(Guid.Empty, guid);
            Assert.AreEqual(guid, Eval<Guid>("convert('" + guid.ToString() + "', 'guid')"));
        }

        [TestMethod]
        public void TestInfinityAndNaN()
        {
            CheckValue(Double.PositiveInfinity, "1.0/0");
            CheckValue(Double.NegativeInfinity, "-1.0/0");
            CheckValue(Double.PositiveInfinity, "1.0/-0");
            CheckValue(Double.NegativeInfinity, "-PositiveInfinity");
            CheckValue(Double.NegativeInfinity, "-1 * PositiveInfinity");
            CheckValue(Double.PositiveInfinity, "-NegativeInfinity");
            CheckValue(Double.PositiveInfinity, "PositiveInfinity * 2");
            CheckValue(Double.PositiveInfinity, "PositiveInfinity + 1");
            CheckValue(Double.NegativeInfinity, "-(PositiveInfinity / 0)");
            CheckValue(Double.PositiveInfinity, "PositiveInfinity / -0");
            CheckValue(.0, "-0 / -PositiveInfinity");
            CheckValue(true, "1.0/0 = PositiveInfinity");
            CheckValue(true, "-1.0/0 = NegativeInfinity");
            CheckValue(true, "PositiveInfinity > NegativeInfinity");
            CheckValue(Double.NaN, "PositiveInfinity + NegativeInfinity");
            CheckValue(Double.NaN, "PositiveInfinity - PositiveInfinity");
            CheckValue(true, "IsInfinity(PositiveInfinity * 2)");
            CheckValue(true, "IsInfinity(cast(PositiveInfinity, 'single') * 2)");
            CheckValue(true, "IsNaN(PositiveInfinity + NegativeInfinity)");
            CheckValue(true, "IsNaN(PositiveInfinity - PositiveInfinity)");
            CheckValue(false, "NaN = NaN");
            Assert.AreEqual(Single.NaN, Eval<float, float>("@context", Single.NaN));
            Assert.IsTrue(Eval<float, bool>("IsNan(cast(@context, 'double'))", Single.NaN));
            Assert.IsTrue(Eval<Double, bool>("IsNan(cast(@context, 'single'))", Double.NaN));
            Assert.IsTrue(Eval<Double, bool>("IsNan(@context)", Double.NaN));
            Assert.IsTrue(Eval<float, bool>("IsInfinity(@context)", Single.PositiveInfinity));
            Assert.IsTrue(Eval<Double, bool>("IsInfinity(@context)", Double.PositiveInfinity));
            Assert.IsTrue(Eval<Double, bool>("1./0 = @context", Double.PositiveInfinity));
            Assert.IsTrue(Eval<Double, bool>("cast(1./0, 'single') = -@context", Double.NegativeInfinity));
            //Assert.IsFalse(Double.NaN == Double.NaN);
        }

        [TestMethod]
        public void TestNull()
        {
            var eval1Result = Eval<int?>("Null");
            Assert.IsFalse(eval1Result.HasValue);

            Assert.AreEqual(1, Eval<int>("1+cast(Null, 'int32')"));
            Assert.AreEqual(1, Eval<int>("1+Convert(Null, 'int32')"));
            Assert.AreEqual(1, Eval<int>("1+Null"));
            
            Assert.IsNull(Eval<string>("Null"));
            Assert.AreEqual("test", Eval<string>("Null + 'test'"));

            var eval4 = (Func<UnboxableNullable<int>, int>)m_runtime.Compile(
                "5 + CASE @arg when 1 then NULL else 25 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));
            
            Assert.AreEqual(5, eval4(1));
            Assert.AreEqual(30, eval4(-2));
            Assert.AreEqual(30, eval4(0.Null()));

            eval4 = (Func<UnboxableNullable<int>, int>)m_runtime.Compile(
                "CASE WHEN IsNull(@arg) then 1 else 2 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));
            
            Assert.AreEqual(2, eval4(1));
            Assert.AreEqual(1, eval4(0.Null()));

            eval4 = (Func<UnboxableNullable<int>, int>)m_runtime.Compile(
                "CASE WHEN @arg iS NuLl then 1 else 2 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));
            
            Assert.AreEqual(2, eval4(1));
            Assert.AreEqual(1, eval4(0.Null()));

            eval4 = (Func<UnboxableNullable<int>, int>)m_runtime.Compile(
                "CASE WHEN @arg between 0 and 0 then 1 else 2 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));
            
            Assert.AreEqual(2, eval4(1));
            Assert.AreEqual(1, eval4(0.Null()));

            eval4 = (Func<UnboxableNullable<int>, int>)m_runtime.Compile(
                "CASE @arg WHEN NULL then 1 else 2 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));
            
            Assert.AreEqual(2, eval4(1));
            Assert.AreEqual(1, eval4(0.Null()));

            var eval5 = (Func<string, int>)m_runtime.Compile(
                "CASE @arg WHEN NULL then 1 else 2 END",
                typeof(int), new Tuple<string, Type>("@arg", typeof(string)));
            
            Assert.AreEqual(2, eval5("test"));
            Assert.AreEqual(2, eval5(string.Empty));
            Assert.AreEqual(1, eval5(null));

            Assert.IsTrue(Eval<UnboxableNullable<int>, bool>("isNull( CASE @context WHEN 1 THEN NULL ELSE NULL END)", 0.Null()));
            Assert.AreEqual(1, Eval<int, int>("1 + CASE @context WHEN 1 THEN NULL ELSE NULL END", 0));
            Assert.AreEqual(1, Eval<int, int>("1 + CASE @context WHEN NULL THEN NULL ELSE NULL END", 0));
            
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,NULL,2 THEN 1 ELSE NULL END", 0));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,NULL,2 THEN 1 ELSE NULL END", 1));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,NULL,2 THEN 1 ELSE NULL END", 2));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,NULL,2 THEN 1 ELSE NULL END", 0.Null()));

            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN NULL,1,2 THEN 1 ELSE NULL END", 0));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN NULL,1,2 THEN 1 ELSE NULL END", 1));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN NULL,1,2 THEN 1 ELSE NULL END", 2));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN NULL,1,2 THEN 1 ELSE NULL END", 0.Null()));
            
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,2,NULL THEN 1 ELSE NULL END", 0));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,2,NULL THEN 1 ELSE NULL END", 1));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,2,NULL THEN 1 ELSE NULL END", 2));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("1 + CASE @context WHEN 1,2,NULL THEN 1 ELSE NULL END", 0.Null()));

            Assert.AreEqual(1, Eval<int, int>("1 + CASE WHEN @context IS NULL THEN NULL ELSE NULL END", 0));
            Assert.AreEqual(6, Eval<UnboxableNullable<int>, int>("1 + CASE WHEN @context IS NULL THEN 5 ELSE NULL END", 0.Null()));
            Assert.AreEqual(6, Eval<UnboxableNullable<int>, int>("1 + CASE WHEN @context IS NOT NULL THEN NULL ELSE 5 END", 0.Null()));

            Assert.AreEqual("test", Eval<string, string>("IfNull(null, 'test')", null));
            Assert.AreEqual(null, Eval<string, string>("IfNull(null, null)", null));
            Assert.AreEqual("test", Eval<string, string>("IfNull('test', 't')", null));
            Assert.AreEqual("test", Eval<string, string>("IfNull(@context, 'test')", null));
            Assert.AreEqual("test", Eval<string, string>("IfNull(@context, 't')", "test"));
            Assert.AreEqual(1, Eval<int>("IfNull(1, 2)"));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("IfNull(@context, 2)", 1));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("IfNull(@context, 2)", 0.Null()));

            Assert.IsTrue(Eval<bool>("IsNull(cast(Null, 'string'))"));
            Assert.IsTrue(Eval<bool>("cast(Null, 'string') is null"));
            Assert.IsTrue(Eval<bool>("IsNull(Null)"));
            Assert.IsFalse(Eval<bool>("nuLl is noT nuLL"));
            Assert.IsTrue(Eval<bool>("Null is null"));
            Assert.IsTrue(Eval<UnboxableNullable<int>, bool>("IsNull(@context)", 0.Null()));
            Assert.IsTrue(Eval<string, bool>("IsNull(@context)", null));
            Assert.IsTrue(Eval<string, bool>("@context is null", null));
            Assert.IsFalse(Eval<string, bool>("IsNull(@context)", "test"));
            Assert.IsFalse(Eval<string, bool>("@context is null", "test"));
            Assert.IsTrue(Eval<string, bool>("@context Is nOt nUll", "test"));

            Assert.AreEqual(false, Eval<UnboxableNullable<bool>, bool>("@context", false.Null()));
            Assert.AreEqual(false, Eval<UnboxableNullable<bool>, bool>("case when @context then true else false end", false));
            Assert.AreEqual(true, Eval<UnboxableNullable<bool>, bool>("case when @context then true else false end", true));
            Assert.AreEqual(0.Null(), Eval<UnboxableNullable<int>, UnboxableNullable<int>>("@context", 0.Null()));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, UnboxableNullable<int>>("1 + @context", 0.Null()));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, UnboxableNullable<int>>("1 + @context", 1));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, UnboxableNullable<decimal>>("null + @context", 1));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, UnboxableNullable<decimal>>("1 + @context", 0.Null()));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, UnboxableNullable<decimal>>("1 + @context", 1));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, UnboxableNullable<decimal>>("@context", 1).Value);
        }

        [TestMethod]
        public void TestCoalesce()
        {
            Assert.AreEqual(1, Eval<int>("coalesce(1, 2)"));
            Assert.AreEqual(null, Eval<string>("coalesce('', '')"));
            Assert.AreEqual(null, Eval<string>("coalesce('', '')"));
            Assert.AreEqual("a", Eval<string>("coalesce('', 'a', 'c')"));
            Assert.AreEqual("b", Eval<string>("coalesce('b', 'a')"));

            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("coalesce(@context, 1)", 0.Null()));
            Assert.AreEqual(2, Eval<UnboxableNullable<int>, int>("coalesce(@context, 0, 2)", 0.Null()));
            Assert.AreEqual(0, Eval<UnboxableNullable<int>, int>("coalesce(@context, 0, 2)", 0));
            Assert.AreEqual(0, Eval<UnboxableNullable<int>, int>("coalesce(0, @context, 2)", 0));
            Assert.AreEqual(2, Eval<int, int>("coalesce(0, @context, 2)", 0));
            Assert.AreEqual(2, Eval<int, int>("coalesce(@context, 0, 2)", 0));
            Assert.AreEqual(2, Eval<int, int>("coalesce(0, @context)", 2));
            Assert.AreEqual(1, Eval<UnboxableNullable<int>, int>("coalesce(@context, 0, 2)", 1));

            Assert.AreEqual(2, ((Func<int, UnboxableNullable<int>, UnboxableNullable<int>>)m_runtime.Compile(
                "coalesce(@val1, @val2)", 
                typeof(UnboxableNullable<int>),
                new Tuple<string, Type>("@val1", typeof(int)),
                new Tuple<string, Type>("@val2", typeof(UnboxableNullable<int>))
                ))(0, 2));

            Assert.AreEqual(1, ((Func<UnboxableNullable<int>, UnboxableNullable<int>, UnboxableNullable<int>>)m_runtime.Compile(
                "coalesce(@val1, @val2)", 
                typeof(UnboxableNullable<int>),
                new Tuple<string, Type>("@val1", typeof(UnboxableNullable<int>)),
                new Tuple<string, Type>("@val2", typeof(UnboxableNullable<int>))
                ))(1, 2));

            Assert.AreEqual(2, ((Func<UnboxableNullable<int>, UnboxableNullable<int>, UnboxableNullable<int>>)
                m_runtime.Compile("coalesce(@val1, @val2)",
                typeof(UnboxableNullable<int>),
                new Tuple<string, Type>("@val1", typeof(UnboxableNullable<int>)),
                new Tuple<string, Type>("@val2", typeof(UnboxableNullable<int>))
                ))(0.Null(), 2));

            Assert.AreEqual(3, ((Func<UnboxableNullable<int>, UnboxableNullable<int>, int, int>)
                m_runtime.Compile("coalesce(@val1, @val2, @val3)",
                typeof(int),
                new Tuple<string, Type>("@val1", typeof(UnboxableNullable<int>)),
                new Tuple<string, Type>("@val2", typeof(UnboxableNullable<int>)),
                new Tuple<string, Type>("@val3", typeof(int))
                ))(0.Null(), 0.Null(), 3));

            Assert.AreEqual("2", Eval<string, string>("coalesce(@context, Default('string'), '2')", ""));
            Assert.AreEqual("2", Eval<string, string>("coalesce(@context, '', '2')", ""));
            Assert.AreEqual("one", Eval<string, string>("coalesce(@context, '', '2')", "one"));
        }

        [TestMethod]
        public void TestEvaluationCorrectness()
        {
            const Boolean boolValue = false;
            var boolAtom = new AtomMetadata(AtomType.Identifier, "boolValue", (Func<object, Boolean>) (_ => boolValue));

            const Int16 int16Value = Int16.MaxValue;
            var int16Atom = new AtomMetadata(AtomType.Identifier, "int16Value", (Func<object, Int16>)(_ => int16Value));

            const Int64 int64Value = 20L;
            var int64Atom = new AtomMetadata(AtomType.Identifier, "int64Value", (Func<object, Int64>)(_ => int64Value));

            const Double doubleValue = 0.1;
            var doubleAtom = new AtomMetadata(AtomType.Identifier, "doubleValue", (Func<object, Double>)(_ => doubleValue));

            const Decimal decimalValue = (decimal)0.1;
            var decimalAtom = new AtomMetadata(AtomType.Identifier, "decimalValue", (Func<object, Decimal>)(_ => decimalValue));

            const string stringValue = "hey";
            var stringAtom = new AtomMetadata(AtomType.Identifier, "stringValue", (Func<object, String>)(_ => stringValue));

            var nullStringAtom = new AtomMetadata(AtomType.Identifier, "nullStringValue", (Func<object, String>)(_ => (string)null));

            m_runtime.RegisterAtom(boolAtom);
            m_runtime.RegisterAtom(int16Atom);
            m_runtime.RegisterAtom(int64Atom);
            m_runtime.RegisterAtom(doubleAtom);
            m_runtime.RegisterAtom(decimalAtom);
            m_runtime.RegisterAtom(stringAtom);
            m_runtime.RegisterAtom(nullStringAtom);

            CheckValue(boolValue, "BoOLvAluE");
            CheckValue(boolValue, "FaLsE");

            CheckValue(boolValue, "boolValue");
            CheckValue(boolValue, "false");
            CheckValue(!boolValue, "true");
            CheckValue(!boolValue, "NOT false");
            CheckValue(!boolValue, "NOT boolValue");

            CheckValue(true, "true AND true");
            CheckValue(false, "false AND true");
            CheckValue(false, "false AND false");
            CheckValue(true, "true OR true");
            CheckValue(true, "false OR true");
            CheckValue(false, "false OR false");
            CheckValue(false, "true XOR true");
            CheckValue(true, "false XOR true");
            CheckValue(false, "false XOR false");
            
            CheckValue(int16Value, "int16Value");
            CheckValue(~int16Value, "~int16Value");
            CheckValue(-int16Value, "-int16Value");
            CheckValue(-int16Value + 2, "-int16Value+2");
            CheckValue(-int16Value - 2, "-int16Value-2");
            CheckValue(-int16Value * 2, "-int16Value*2");
            CheckValue(-int16Value / 2, "-int16Value/2");
            CheckValue(-int16Value % 2, "-int16Value%2");

            CheckValue(-int16Value + 2.0, "-int16Value+2.0");
            CheckValue(-int16Value - 2.0, "-int16Value-2.0");
            CheckValue(-int16Value * 2.0, "-int16Value*2.0");
            CheckValue(-int16Value / 2.0, "-int16Value/2.0");
            CheckValue(-int16Value % 2.0, "-int16Value%2.0");

            CheckValue(int64Value, "int64Value");
            CheckValue(~int64Value, "~int64Value");
            CheckValue(-int64Value, "-int64Value");
            CheckValue(-int64Value + 2, "-int64Value+2");
            CheckValue(-int64Value - 2, "-int64Value-2");
            CheckValue(-int64Value * 2, "-int64Value*2");
            CheckValue(-int64Value / 2, "-int64Value/2");
            CheckValue(-int64Value % 2, "-int64Value%2");

            CheckValue(doubleValue, "doubleValue");
            CheckValue(-doubleValue, "-doubleValue");
            CheckValue(-doubleValue + 2, "-doubleValue+2");
            CheckValue(-doubleValue - 2, "-doubleValue-2");
            CheckValue(-doubleValue * 2, "-doubleValue*2");
            CheckValue(-doubleValue / 2, "-doubleValue/2");
            CheckValue(-doubleValue % 2, "-doubleValue%2");

            CheckValue(decimalValue, "decimalValue");
            CheckValue(-decimalValue, "-decimalValue");
            CheckValue(-decimalValue + 2, "-decimalValue+2");
            CheckValue(-decimalValue - 2, "-decimalValue-2");
            CheckValue(-decimalValue * 2, "-decimalValue*2");
            CheckValue(-decimalValue / 2, "-decimalValue/2");
            CheckValue(-decimalValue % 2, "-decimalValue%2");

            CheckValue(int64Value + int16Value, "int64Value + int16Value");
            CheckValue(int64Value - int16Value, "int64Value - int16Value");
            CheckValue(int64Value / int16Value, "int64Value / int16Value");
            CheckValue(int16Value / int64Value, "int16Value / int64Value");
            CheckValue(int64Value * int16Value, "int64Value * int16Value");

            CheckValue(int64Value + decimalValue, "int64Value + decimalValue");
            CheckValue(int64Value - decimalValue, "int64Value - decimalValue");
            CheckValue(int64Value / decimalValue, "int64Value / decimalValue");
            CheckValue(int64Value * decimalValue, "int64Value * decimalValue");

            CheckValue(((decimal)doubleValue) + decimalValue, "doubleValue + decimalValue");
            CheckValue(((decimal)doubleValue) - decimalValue, "doubleValue - decimalValue");
            CheckValue(((decimal)doubleValue) / decimalValue, "doubleValue / decimalValue");
            CheckValue(((decimal)doubleValue) * decimalValue, "doubleValue * decimalValue");

            CheckValue(decimalValue + ((decimal)doubleValue), "decimalValue + doubleValue");
            CheckValue(decimalValue - ((decimal)doubleValue), "decimalValue - doubleValue");
            CheckValue(decimalValue / ((decimal)doubleValue), "decimalValue / doubleValue");
            CheckValue(decimalValue * ((decimal)doubleValue), "decimalValue * doubleValue");

            CheckValue(true, "1 = 1");
            CheckValue(true, "1.1 = 1.1");
            CheckValue(true, "true = tRuE");
            CheckValue(true, "false = fAlsE");
            CheckValue(true, "boolValue = boolValue");
            CheckValue(true, "boolValue = false");
            CheckValue(false, "NOT (boolValue = false)");
            CheckValue(true, "NOT boolValue = true");
            CheckValue(true, "(NOT boolValue) = true");
            CheckValue(true, "(NOT (NOT boolValue)) = false");
            CheckValue(true, "stringValue = stringValue");
            CheckValue(true, "stringValue = '" + stringValue + "'");
            CheckValue(true, "nullStringValue = nullStringValue");
            CheckValue(true, "nullStringValue = ''");
            CheckValue(true, "IsDefault(nullStringValue)");
            CheckValue(true, "IsDefault('')");
            Assert.IsTrue(m_runtime.Compile<string, bool>("IsDefault(@context)")(null));
            Assert.IsTrue(m_runtime.Compile<string, bool>("IsDefault(@context)")(string.Empty));
            Assert.IsTrue(m_runtime.Compile<string, bool>("IsDefault(@context)")(""));
            CheckValue(true, "IsDefault(0)");
            CheckValue(true, "IsDefault(0.0)");
            CheckValue(true, "IsDefault(false)");
            CheckValue(false, "Default('boolean')");
            CheckValue((string)null, "Default('string')");
            CheckValue((SizableArrayOfByte)null, "Default('binary')");
            CheckValue(0, "Default('int16')");
            CheckValue(0, "Default('int32')");
            CheckValue((Int64)0, "Default('int64')");
            CheckValue(0.0, "Default('double')");
            CheckValue((decimal)0.0, "Default('decimal')");
            CheckValue(true, "1 != 0");
            CheckValue(int16Value + int64Value > 1, "int16Value + int64Value > 1");
            CheckValue(int16Value + int64Value > 1.0, "int16Value + int64Value > 1.0");
            CheckValue(decimalValue + int16Value + int64Value, "decimalValue + int16Value + int64Value");
            CheckValue(int16Value + int64Value + decimalValue > (decimal)1.0, "int16Value + int64Value + decimalValue > 1.0");
            CheckValue(true, "1.1 != 2.2");
            CheckValue(true, "1 > 0");
            CheckValue(true, "1 >= 0");
            CheckValue(true, "1 !< 0");
            CheckValue(false, "1 < 0");
            CheckValue(false, "1 <= 0");
            CheckValue(false, "1 !> 0");

            CheckValue("haha", "'haha'");
            CheckValue("haha" + "huhu", "'haha' + 'huhu'");
            CheckValue("haha" + "huhu" + "hehe", "'haha' + 'huhu' + 'hehe'");
            CheckValue(stringValue + "huhu" + "hehe", "stringValue + 'huhu' + 'hehe'");
            CheckValue(true, "Contains(stringValue, 'hEy')");
            CheckValue(true, "Contains(stringValue, 'eY')");
            CheckValue(true, "Contains(stringValue, 'He')");
            CheckValue(false, "Contains(nullStringValue, 'he')");
            CheckValue(false, "Contains('he', nullStringValue)");
            CheckValue(true, "StartsWith(stringValue, 'he')");
            CheckValue(false, "StartsWith(nullStringValue, 'he')");
            CheckValue(false, "StartsWith('he', nullStringValue)");
            CheckValue(false, "StartsWith('he', 'z')");
            CheckValue(true, "EndsWith(stringValue, 'eY')");
            CheckValue(false, "EndsWith('', 'aha')");
            CheckValue(false, "EndsWith(nullStringValue, 'aha')");
            CheckValue(false, "EndsWith('aha', nullStringValue)");
            CheckValue(true, "'aha' = 'AHA'");
            CheckValue(true, "'aha' != 'AHA1'");
            CheckValue(true, "'aha' <> 'AHA1'");
            CheckValue(true, "'aha' > 'aBA'");
            CheckValue(true, "'aha' < 'aJa'");
            CheckValue(true, "'aha' < 'aJa'");
            CheckValue(true, "'aha' <= 'aHa'");
            CheckValue(true, "'aha' !> 'aHa'");
            CheckValue(true, "'aha' <= 'aJa'");
            CheckValue(true, "'aha' !> 'aJa'");
            CheckValue(true, "'aha' >= 'aha'");
            CheckValue(true, "'aha' !< 'aha'");
            CheckValue(true, "'aha' >= 'aBA'");
            CheckValue(true, "'aha' !< 'aBA'");
            
            CheckValue(true, "'aha' IN ('aha', 'aba')");
            CheckValue(true, "'aha' IN ('AHA', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11')");
            CheckValue(true, "'aha' IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', 'AHA')");
            CheckValue(true, "'aha' IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', 'AHA', 'aHa', 'AHa')");
            CheckValue(true, "1 in (3, 4, 5, 1, 6, 7)");
            Assert.IsTrue(Eval<int, bool>("@context in (3, 4, 5, 1, 6, 7, 8, 9, 10, 11, 12)", 1));
            Assert.IsFalse(Eval<int, bool>("@context in (3, 4, 5, -1, 6, 7, 8, 9, 10, 11, 12)", 1));
            CheckValue(true, "(1.5 * 2) in (3, 4, 5)");
            CheckValue(false, "(1.5 * 2) NOT in (3, 4, 5)");
            CheckValue(false, "(1.6 * 2) in (3, 4, 5)");
            CheckValue(true, "(1.6 * 2) in (3, 3.2, 5)");
            CheckValue(false, "(1.6 * 2) not IN (3, 3.2, 5)");
            CheckValue(true, "(1.6 * 2) in (1, 2, '3', 4, -5, 6, 7, 8, 9, 10, 11, 3.2, 5)");

            CheckValue("haha" + int16Value, "'haha' + Convert(int16Value, 'String')");
            CheckValue(int16Value + "haha", "Convert(int16Value, 'String') + 'haha'");
            CheckValue("haha" + int64Value, "'haha' + Convert(int64Value, 'String')");
            CheckValue(int64Value + "haha", "Convert(int64Value, 'String') + 'haha'");
            CheckValue("haha" + doubleValue, "'haha' + Convert(doubleValue, 'string')");
            CheckValue(doubleValue + "haha", "Convert(doubleValue, 'string') + 'haha'");
        }

        private void CheckValue<T>(T expected, string text)
        {
            var evaluator = m_runtime.Compile<object, T>(text);
            var result = evaluator(null);
            Assert.AreEqual(expected, result);

            Debug.WriteLine(text + " = " + result);

            if (!ReferenceEquals(result, null))
            {
                var systemType = typeof(T);
                Assert.AreSame(result.GetType(), systemType);
            }
        }

        [TestMethod]
        public void Examples()
        {
            Assert.AreEqual(1.5*2.6, ((Func<Double>)m_runtime.Compile("1.5 * 2.6", typeof(Double)))());
            Assert.AreEqual(-1, ((Func<int>)m_runtime.Compile("1 + 2*(3-4)", typeof(Int32)))());
            Assert.AreEqual(Double.NegativeInfinity, ((Func<Double>)m_runtime.Compile("-1.0/0", typeof(Double)))());
            Assert.IsTrue(((Func<bool>)m_runtime.Compile("1 > -1.5", typeof(bool)))());
            Assert.IsTrue(((Func<bool>)m_runtime.Compile("'xyz' > 'abc'", typeof(bool)))());
            Assert.IsTrue(((Func<bool>)m_runtime.Compile("false OR true", typeof(bool)))());
            Assert.IsTrue(((Func<bool>)m_runtime.Compile("false XOR true", typeof(bool)))());
            Assert.IsTrue(((Func<bool>)m_runtime.Compile("NOT false", typeof(bool)))());

            var eval4 = (Func<Single, bool>)m_runtime.Compile("@arg1 = cast(1+0.5, 'int32')",
                typeof(Boolean), new Tuple<string, Type>("@arg1", typeof(Single)));
            Assert.IsFalse(eval4(1.5f));
            Assert.IsFalse(eval4(1.4f));
            Assert.IsTrue(eval4(1f));

            var eval5 = (Func<string, int>) m_runtime.Compile("cast(convert(@var, 'Double') * 2, 'int32')", typeof(int), 
                new Tuple<string, Type>("@var", typeof(string)));
            Assert.AreEqual(8, eval5("4"));
            Assert.AreEqual(8, eval5("4.1")); 

            var eval6 = (Func<int, string>) m_runtime.Compile("convert(@var*2, 'string') + 'xyz'", typeof (string), 
                new Tuple<string, Type>("@var", typeof (int)));
            Assert.AreEqual("10xyz", eval6(5));

            var eval7 = m_runtime.Compile<int, bool>("@Context = 1");
            Assert.IsTrue(eval7(1));
            Assert.IsFalse(eval7(2));

            var testData = new TestData {Int64Field1 = 25};
            var eval8 = (Func<TestData, int, bool>) m_runtime.Compile("Int64Field1 = @arg", typeof(bool), 
                new Tuple<string, Type>("@Context", typeof(TestData)), new Tuple<string, Type>("@arg", typeof(Int32)));
            Assert.IsTrue(eval8(testData, 25));
            Assert.IsFalse(eval8(testData, 26));
        }

        [TestMethod]
        public void TestContract()
        {
            var data = new SomeDataObject
                {
                    Id = 1,
                    Name = "test",
                    Status = 3,
                    Number = "number"
                };

            Func<SomeDataObject, SomeDataObject> stateless = x => x;
            Func<SomeDataObject> stateful = () => data;
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Identifier, "Contract", stateless));
            m_runtime.RegisterAtom(new AtomMetadata(AtomType.Identifier, "ContractCaptured", stateful));

            // where Contract.Id = 1
            Assert.IsTrue((m_runtime.Compile<SomeDataObject, bool>("ID = 1"))(data));
            Assert.IsTrue((m_runtime.Compile<SomeDataObject, bool>("Contract.ID = 1"))(data));
            Assert.IsTrue((Eval<bool>("ContractCaptured.ID = 1")));
            Assert.AreEqual(1, (Eval<UnboxableNullable<long>>("ContractCaptured.ID")));
            // where Contract.Name = 'test'
            Assert.IsTrue((m_runtime.Compile<SomeDataObject, bool>("Name = 'test'"))(data));
            // where Contract.Name like '%t%'
            // where Contract.Number like '%x%' AND Contract.Status = 2
            // where Contract.Status = 3 Order by Contract.Name ASC, Contract.Number DESC
            Assert.IsFalse((m_runtime.Compile<SomeDataObject, bool>("Contains(Number, 'x') AND status = 2"))(data));
        }

        [TestMethod]
        public void TestSetContains()
        {
            var stringeval = (Func<HashSet<string>, string, bool>)m_runtime.Compile(
                "SetContains(@set, @arg)", typeof (bool),
                new Tuple<string, Type>("@set", typeof (HashSet<string>)),
                new Tuple<string, Type>("@arg", typeof(string)));

            var stringset = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {"aa", "bb", "cc"};
            Assert.IsTrue(stringeval(stringset, "aa"));
            Assert.IsTrue(stringeval(stringset, "AA"));
            Assert.IsFalse(stringeval(stringset, null));
            Assert.IsFalse(stringeval(stringset, "ab"));

            stringset.Add(null);
            Assert.IsTrue(stringeval(stringset, null));

            var intseteval = (Func<HashSet<int>, int, bool>)m_runtime.Compile(
                "SetContains(@set, @arg)", typeof (bool),
                new Tuple<string, Type>("@set", typeof (HashSet<int>)),
                new Tuple<string, Type>("@arg", typeof(int)));

            var intset = new HashSet<int> {1, 2, 3};
            Assert.IsTrue(intseteval(intset, 1));
            Assert.IsFalse(intseteval(intset, -1));

            var intsetneval = (Func<HashSet<UnboxableNullable<int>>, int, bool>)m_runtime.Compile(
                "SetContains(@set, @arg)", typeof(bool),
                new Tuple<string, Type>("@set", typeof(HashSet<UnboxableNullable<int>>)),
                new Tuple<string, Type>("@arg", typeof(int)));

            var intsetn = new HashSet<UnboxableNullable<int>> { 1, 2, 3, 0.Null() };
            Assert.IsTrue(intsetneval(intsetn, 1));
            Assert.IsFalse(intsetneval(intsetn, -1));

            var intsetnneval = (Func<HashSet<UnboxableNullable<int>>, UnboxableNullable<int>, bool>)m_runtime.Compile(
                "SetContains(@set, @arg)", typeof(bool),
                new Tuple<string, Type>("@set", typeof(HashSet<UnboxableNullable<int>>)),
                new Tuple<string, Type>("@arg", typeof(UnboxableNullable<int>)));

            var intsetnn = new HashSet<UnboxableNullable<int>> { 1, 2, 3 };
            Assert.IsTrue(intsetnneval(intsetnn, 1));
            Assert.IsFalse(intsetnneval(intsetnn, -1));
            Assert.IsFalse(intsetnneval(intsetnn, 0.Null()));

            intsetnn.Add(0.Null());
            Assert.IsTrue(intsetnneval(intsetnn, 0.Null()));

            var intsetnnevalsbyte = (Func<HashSet<UnboxableNullable<int>>, UnboxableNullable<SByte>, bool>)m_runtime.Compile(
                "SetContains(@set, @arg)", typeof(bool),
                new Tuple<string, Type>("@set", typeof(HashSet<UnboxableNullable<int>>)),
                new Tuple<string, Type>("@arg", typeof(UnboxableNullable<SByte>)));

            Assert.IsTrue(intsetnnevalsbyte(intsetnn, 1));
            Assert.IsFalse(intsetnnevalsbyte(intsetnn, (-1)));
            Assert.IsTrue(intsetnneval(intsetnn, 0.Null()));
        }

        [TestMethod]
        public void ParserDemo()
        {
            var runtime = new ExpressionEvaluatorRuntime();
            Func<object, string> status = x => "booked";
            runtime.RegisterAtom(new AtomMetadata(AtomType.Identifier, "status", status));
            var tree = runtime.Parse("status in ('booked', 'bound')", CancellationToken.None);
            runtime.Analyze(tree.Root, new CompilerState(runtime, typeof(object), typeof (bool)));
            Iterate(tree.Root, 0);
        }

        private void Iterate(ParseTreeNode node, int level)
        {
            Console.WriteLine(new string(' ', level * 2) + node);
            foreach (var child in node.ChildNodes)
            { Iterate(child, level + 1); }
        }

        private TReturn Eval<TReturn>(string expression)
        {
            return ((Func<TReturn>)m_runtime.Compile(expression, typeof(TReturn)))();
        }

        private TReturn Eval<TContext, TReturn>(string expression, TContext ctx)
        {
            return m_runtime.Compile<TContext, TReturn>(expression)(ctx);
        }
    }
}
