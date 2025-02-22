import bcrypt



print(bcrypt.hashpw('superSecure'.encode('utf-8'), bcrypt.gensalt(rounds=5)))

$2b$05$5qjdumS7NHSPxyoz9gXOROLdx8PJ3TWuYkuZIGO6RcS2aPeH81n3i