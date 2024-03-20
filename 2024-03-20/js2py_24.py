from js2py import eval_js
js_code = """ 
    function addNumbers(a, b)
        {return a + b;}
    """
add_numbers = eval_js(js_code)
result = add_numbers(5, 3)
print("Result:", result)
Result: 8


from js2py import eval_js

 js_code='''
   function factorial(n) {
        if (n === 0) {
             return 1;
        } else {
        return n * factorial(n - 1);
        }
   }
    '''
factorial_py = eval_js(js_code)
result = factorial_py(5)
print("Factorial of 5:", result)
Factorial of 5: 120