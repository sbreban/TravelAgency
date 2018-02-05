# TravelAgency

Modele Formale de Concurenta si Comunicatii, Babeș - Bolyai University, Cluj - Napoca

 Proiect - o aplicatie concurenta distribuita la alegerea studentului. Aplicatia trebuie sa respecte urmatoarele cerinte:
- sa fie distribuita, client-server sau web, pe mai multe nivele (client/web - business/middleware - date etc.).
- sa implice aspecte de concurenta la nivel de date externe manipulate (i.e. tranzactii in baze de date).
- sa foloseasca doua baze de date diferite (cel putin 3 tabele) si sa se foloseasca tranzactii distribuite - adica o tranzactie sa lucreze pe tabele din baze de date diferite. (nu este obligatoriu sa fie 2 servere distincte de baze de date, doar 2 baze de date/scheme diferite)
- sa aiba cel putin 6-8 operatii/cazuri de utilizare.
- Foarte important: Sa implementeze un sistem tranzactional la nivel aplicatie. Adica veti considera ca o tranzactie nu consta din operatii de read() si write() de pagini de memorie asa cum facem la curs ci veti considera o tranzactie ca fiind formata din operatii SQL simple (minim 3 instructiuni SQL - insert, delete, update, select) desigur, aceste operatii SQL vor opera pe tabele diferite. Trebuie sa asigurati, la nivel aplicatie, proprietatile ACID ale acestei tranzactii. Cu alte cuvinte sa se implementeze urmatoarele:

    un algoritm de planificare (i.e. algoritm de controlul concurentei) din cele discutate la curs (bazat pe blocari sau pe ordonari, timestamp-uri etc.); aplicatia sa foloseasca 2 baze de date. Cei care implementati planificare bazata pe ordonarea timestamp-urilor trebuie sa implementati obligatoriu si un mecanism de multiversionare si sa reporniti automat orice tranzactie la care planificatorul ii da abort.
    un mecanism de rollback discutat la curs (multivesiuni, rollback pentru fiecare instructiune SQL simpla etc.)
    un mecanism de commit (poate fi gandit impreuna cu cell de rollback)
    un mecanism de detectie si rezolvare a deadlock-urilor (grafuri/liste de conflicte etc.)

- Atentie:: Focus-ul aplicatiei trebuie sa cada pe implementarea sistemului tranzactional, nu pe cazuri de utilizare, intrefata web sau frameworkuri pe care le-ati folosit. Puteti folosi framework-uri care sa va usureze munca (e.g. Hibernate sau alt JPA, Spring, .NET MVC etc.), dar nu trebuie sa folositi nici un fel de suport tranzactional de la acestea. 
