import ast
import sys

code = sys.stdin.read()

tree = ast.parse(code)

def remove_docstrings(node):
    if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)) and node.body:
        if isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Str):
            node.body.pop(0)
    for child in ast.iter_child_nodes(node):
        remove_docstrings(child)

remove_docstrings(tree)

output = ast.unparse(tree)

print(output)