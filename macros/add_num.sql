{#-- macro syntax#}
{#-- macro is block of code, which can be re-used.#}

{{ % macro add_num (num1, num2, num3) % }}

{{ num1 }}+{{num2}}+{{num3}}

{{ % endmacro % }}