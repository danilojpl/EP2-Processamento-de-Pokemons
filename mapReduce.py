from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class ListaPokemons(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1)
            ,MRStep(mapper=self.mapper_2,
            reducer = self.reducer_1),
            MRStep(reducer=self.reducer_2)
            ,MRStep(reducer=self.reducer_3)

        ]

    def mapper_1(self, _, linha):
        dados = linha.split("}")
        for pokemon in dados :
            if(pokemon !="\""):
                pokemon = pokemon.replace('\'', '')
                tipos = re.findall(r"\[([\s:,\w]+)\]", pokemon)
                danos = re.findall(r"\{([\w:\s*,\.]+)", pokemon)
                atributos = pokemon.split(",")
                yield int(float(atributos[0])), {"nome":atributos[1],"tipos":tipos, "danos": danos}
    
    def mapper_2(self, chave, valores):
            tipos = len(valores["tipos"])
            danos = valores["danos"]
            str1 = ''.join(danos)
            str1 = re.sub("([:]+)", ' -', str1)
            str1 = re.sub("([\*]+)", '', str1)
            result = dict((a.strip(), float(b.strip()))  
                     for a, b in (element.split('-')  
                                  for element in str1.split(', ')))
            yield 2,{"tipo":"adversario","nome": valores["nome"],"danos": result}
            yield 2,{"tipo":"atacante","nome": valores["nome"],"tipos":valores["tipos"][tipos-1].split(", ")}

    def reducer_1(self, chave, valores):
        pokemon_danos = []
        pokemon_tipos = []

        for valor in valores:
            if (valor["tipo"] == "adversario"):
                pokemon_danos.append({"nome":valor["nome"],"danos":valor["danos"]})
            elif(valor["tipo"] == "atacante"):
                pokemon_tipos.append({valor["nome"]:valor["tipos"]})
        
        for pokemon in pokemon_danos:
            for p in pokemon_tipos:
                if(list(p)[0]!=pokemon["nome"]):
                    pokemon["list_pokemon"] = p
                    yield None, pokemon

    def reducer_2(self, chave, valores):
        lista = []
        dict_danos = {}
        for i in valores:
            nome = list(i["list_pokemon"])[0]
            lista = list(dict.fromkeys(list(i["list_pokemon"].values())[0]))
            dict_danos = i["danos"]
            res = [dict_danos[i] for i in lista if i in dict_danos]
            yield i["nome"], ('%02f'%float(sum(res)),nome)

    def reducer_3(self,chave,valores):
        self.alist = []
        for valor in valores:
            self.alist.append(valor)
        self.blist = []
        for i in range(10):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        for i in range(10):
            yield chave, self.blist[i]
        




if __name__ == '__main__':
    ListaPokemons.run()