Expression Evaluation Engine
============================

C#.NET dynamic expressions parser and compiler

# E3 Purpose and Scope

Expressions evaluation engine (E3) can support querying, filtering and some other use cases for expressions evaluation:

- Ad-hoc conditions, applied on arbitrary objects

  - Applications: precomputed filters, object stream processing, rule-based ACLs

  - Objects: domain data, wire data contracts, arbitrary values

- SQL-like query processors

  - Applications: Where, Select, Update … Set, Order By, Group By, aggregates

  - Fields, input arguments, arbitrary constants and non-deterministic functions

SQL-like query processors can extend and use E3 engine at run-time to support advanced features such as various flavors of IN operator, expressions in the select statement etc. Single query may make use of multiple individual expressions, combining and evaluating them according to appropriate execution plan. E3 itself will only provide a generic execution engine, with support for:

- Arguments of arbitrary types

- Custom N-ary functions and atoms

- Custom extensions for the analyzer (allowing injection of custom ExpressionTrees)

- Complete runtime configuration, no need to re-compile the engine code when adding custom extensions

First goal for E3 is to deliver maximum speed for repetitive evaluations, e.g. where the same expression is evaluated over hundreds of millions of input values. Hence the three implementation requirements:

- Emit binary code at runtime

- Emit thread-safe stateless code

- Avoid dynamic memory allocation inside the evaluator, unless explicitly required by expression

Second goal for E3 is smooth integration with C# code, data structures and garbage collector. Thus, implementation is built in C#, using Expression Trees and garbage-collectible lambdas (Funcs and Actions), and compilation APIs must support heavily concurrent clients.

Engine’s input is its runtime configuration (factory object) and a text string for parsing, analysis and compilation.

Outputs are a parse tree (AST – abstract syntax tree), Expression tree, and a compiled .NET lambda.

# Language & APIs

E3 is distributed as a .NET Standard assembly DLL, paired with 3-rd party parsing library (NuGet package of [https://www.nuget.org/packages/Irony](https://www.nuget.org/packages/Irony), source at [https://github.com/IronyProject/Irony](https://github.com/IronyProject/Irony)), debug symbols and XML comments files.

E3’s application programming interface exposes:

- Parsing library dependency: `ParseTree, ParseTreeNode`

  - Objects of these types support parser’s and compiler’s extensibility features

- Runtime factory object: interface `IExpressionEvaluatorRuntime`

  - Implemented by public type `ExpressionEvaluatorRuntime`

  - Registration of custom atoms, either as pre-compiled lambdas or as Expression generators (injection of Expression Trees)

  - Parsing an expression text, produces `ParseTree`

  - Analyzing `ParseTree`, produces `LambdaExpression`

  - Compiling `LambdaExpression` into `Func` or `Action`

- Ad-hoc functor signature convention for general-purpose expressions

  - `Func<…>, Action<…>`, with zero or more arguments

- Recommended functor signature convention for certain kinds of extensions

  - `Func<TContext, T>`, where TContext is any type

  - Predefined name for the context argument, `@Context`

  - Apply when the number of arguments passed into expression may be very large (but not all of them are used at the same time) to reduce the overhead

  - Also helps to improve maintainability of client code

# Available Data Types

## Usage Rules

In scope of expression engine, data types are driven by .NET framework’s native value types and reference types. In external application such as SQL-like query processor the data type system will be built around System.Data.DbType enumeration, and its values are mapped onto .NET native data types.

When casting or converting values, you supply desired data type’s name in single quotes. These string names are automatically prepended with `System.` and mapped onto .NET CLR native types. If the data type you reference by name in quotes is not part of System namespace, you will get a compile-time error.

Expression Engine itself does not require arguments to be of certain types unless you ask it to execute some operation. In the context of some operator or function, expression may try to cast your argument to required type and may fail either at compile time or at run time.

Many data types are not supported by any E3 operator or function. Nevertheless, you can reference atoms that return values of those types. Type restrictions are only applied when try to do something with the value other than just passing it around (e.g. when you apply cast, convert, arithmetic etc.).

String comparisons are ordinal, case-insensitive, culture-agnostic. Beware, “culture-agnostic” is different than “culture-invariant”: we use .NET’s native `StringComparer.OrdinalIgnoreCase` to maximize performance, and this may produce undesirable results in certain special situations with Unicode characters, but is sufficient for majority of cases.

| Size    | Types   
|---------|---------
|8-bit    |`Byte, SByte, Boolean`
|16-bit   |`Int16, UInt16, Char`
|32-bit   |`Int32, UInt32, Single`
|64-bit   |`Int64, UInt64, Double, DateTime, TimeSpan`
|128-bit  |`Decimal, Guid`
|Reference|`Object, String`

# Reference List of Predefined Operators and Functions

### Unary Operators

| Operator   |  Category   | Notes
|------------|-------------|-------
| -          | Negates operand | Numeric
| +          |Decorates positive numeric value|Numeric
| ~          |Bitwise complement | Integer
| NOT        |Boolean negation| Boolean
| IS NULL    |Test for null value (not same as test for empty/default!)| Boolean
|IS NOT NULL |Test for not-null value (not same as test for non-empty/non-default!)|Boolean

### Binary Operators

| Operator        |  Category            | Notes
|-----------------|----------------------| ----------------------
| +, -, *, /, %   |      Arithmetics     |        Numeric and Numeric
| +               | Non-numeric addition or concatenation | String and String, DateTime and TimeSpan, TimeSpan and TimeSpan, TimeSpan and DateTime
| -               | Non-numeric subtraction | DateTime and DateTime, DateTime and TimeSpan
|=, >=, <=, <>, != , !>, !<| Comparison, yields Boolean| String and String (case-insensitive), Numeric and Numeric, DateTime and DateTime
|AND, OR, XOR     |  Boolean             | Boolean and Boolean
| &, \|, ^        |  Bitwise arithmetics | Numeric and Numeric

### Other Operators

##### **CASE** [arg] **WHEN** [val1] **THEN** [expr1] **ELSE** [expr2] **END**

Analog of switch statement in C#

Supports multiple values in WHEN, such as `WHEN 1,2,3 THEN ...`

Data types of expr1…exprN must match data type of arg

##### **CASE** **WHEN** [cond1] **THEN** [expr2] **ELSE** [expr3] **END**

Analog of series of if-then-else statements in C#

Data types of cond1…condN must be Boolean

Data types of expr1…exprN must match

##### [arg] **IN** ([set]), [arg] **NOT IN** ([set])

Returns Boolean true or false, depending on whether argument equals one of the set

[set] must be an explicit comma-separated list of constant literals of numeric or string type

##### [arg] **BETWEEN** [left] **AND** [right], [arg] **NOT BETWEEN** [left] **AND** [right]

Translates to either [arg] >= [left] AND [arg] <= [right], or [arg] < [left] OR [arg] > [right]

Data types of [arg], [left] and [right] must match

### Functions & Literals

| Syntax | Notes 
| --------|---------
|NULL | Untyped NULL literal
| True, False | Predefined Boolean constant literals
| IsNull ([arg])|Same as `[arg] IS NULL`
| IfNull ([arg], [def])| If arg is null, returns def, otherwise returns arg
|Coalesce ([arg], [def1], [def2], ...)|Returns the first non-null argument from the list. Up to 20 alternatives
|IsDefault ([arg])|Returns true if argument equals default value for its data type (zero for numeric or Guid, false for Boolean, nil pointer for string and binary, zero date for datetime)
|Default (‘[type]’)|Returns default value of supplied data type (zero for numeric or Guid, false for Boolean, nil pointer for string and binary, zero date for datetime)
|IsNaN ([arg])|Returns true if argument is not a number (only for Single and Double floating point values)
|IsInfinity ([arg])|Returns true if argument is a positive or negative infinity (only for Single and Double floating point values)
|PositiveInfinity ([arg])|Returns positive infinity value (Double floating point value)
|NegativeInfinity ([arg])|Returns negative infinity value (Double floating point value)
|Convert ([arg], ‘[type]’)|Attempts to convert value of one data type into value of another data type. Arguments can be of any type, will throw run-time error if types are incompatible.
|Cast ([arg], ‘[type]’)|Attempts to cast value of one data type into value of another data type. Arguments must be of compatible types. At this time, use it to cast between numeric types only.
|StartsWith ([str], [prefix])|Returns true if first argument begins with second argument. Arguments must be strings. Case-insensitive using `OrdinalIgnoreCase`.
|EndsWith ([str], [suffix])|Returns true if first argument ends with second argument. Arguments must be strings. Case-insensitive using `OrginalIgnoreCase`.
|Contains ([str], [infix])|Returns true if first argument contains, begins with or ends with second argument. Arguments must be strings. Case-insensitive using `OrginalIgnoreCase`.
|ToDateTime ([str], [fmt])|Parses first string argument into datetime using format string (second argument).
|ToDateTime ([year], [month], [day], [hour], [minute], [second])|Produces a datetime out of six integers.
|ToDateTime([int64])|Produces a datetime out of Int64 value previously generated by DateTime.ToBinary() – in order to preserve UTC ticks/kind marker.

# Handling NULL

### Kinds of NULL

Null values come in different flavors.  First, there is a “NULL” literal, so that you can write UPDATE statement that sets some field to NULL, or maybe some expression that returns NULL. Also, a NULL value may exist implicitly and E3 lets you test any expression for being NULL.

Technically, NULL literal is an untyped value. E3 compiler automatically translates NULL literals into the default empty constant value for appropriate type, and the type is usually derived from context. Internally, NULL literal is passed around as a special type called `VoidMarker` – because NULL literal is indeed untyped and void.

NULL values are represented in different ways for reference and value types. For reference types, NULL value is passed around as a nil pointer. For value types, there is a utility generic structure called `UnboxableNullable<T>` that can be instantiated with any value type. When you use NULL literal, compiler will instantiate an `UnboxableNullable<VoidMarker`> and pass it around as-is until it is able to derive appropriate type to cast to: for reference type it will get transformed into “null” constant, for value type it will be transformed into `default(UnboxableNullable<T>)`.

There is a reason not to use .NET’s standard `Nullable<T`> structure to pass nullable-type values. When boxed into object, standard nullables are represented as a nil pointer and valuable state information is lost. This particular boxing issue is a problem for E3 compiler, because it needs to manipulate `ConstantExpression` objects holding null nullable values without loss of state. E3 compiler also never uses .NET boxing, so there is no performance impact caused by use of special nullable generic.

### Semantics of using NULL

|E3 expression|Return type
|--------------|------------
|NULL|`UnboxableNullable<VoidMarker>`
|Binary and unary operators with NULL literal or nullable types| Always non-nullable value or reference type, e.g. `UnboxableNullable` is stripped off, nullable-type values are replaced with default values of appropriate type
|CASE with NULL literal or nullable types|`UnboxableNullable<T>`, e.g. compiler replaces constant of type  with proper value of appropriate type – as soon as it knows what type to use
|Function calls with NULL literal or nullable types|Defined by function’s signature

# Use Cases

For a comprehensive list of examples, please refer to the unit test project.

Brief overview:
- arithmetics, NaN, defaults and NULL, all functions
- all flavors of `CASE` and `BETWEEN`
- parameterization and evaluation context
- extending runtime (supplying external `Func` objects for symbol implementations)
- extending analyzer (supplying custom `Expression` generators for sub-trees of the AST)

# Implementation Details – CURRENT STATE, MAY CHANGE

E3 implements three operations

- Parsing, produces AST. To parse expression text, E3 uses open-source .NET library called Irony ([http://irony.codeplex.com/](http://irony.codeplex.com/) ).

- Analysis, produces `Expression`. Analyzer takes AST and runtime configuration, and emits Expression tree with explicit data types (e.g. no implicit CLR boxing).

- Compilation, produces an object, which is in fact a constructed `Func` or `Action`. Compiler takes an Expression and compiles it into a lambda using argument declarations from `CompilerState`. The reason to declare return type as an object is to help reduce the runtime overhead from generalization of the runtime interface.

Parser uses an adapted version of SQL-89 grammar (a sample that comes with Irony). The grammar is not exposed via APIs.

Detailed information on each public type is contained within XML comments.

Division by zero, as-implemented now (follows .NET rules at this time)

1) `1.0/0 = PositiveInfinity` (a valid `Double` value, and you can use this literal in E3 expression)

2) `-1.0/0 = NegativeInfinity` (a valid `Double` value, and you can use this literal in E3 expression)

3) `1/0` = runtime error, division by zero (because 1 is integer)