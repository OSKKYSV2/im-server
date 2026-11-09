package utb.fai;

import java.util.*;

/**
 * Sdílený stav serveru: připojení klienti, jejich jména a členství v místnostech.
 * Všechny metody jsou synchronizované, protože HashSet a HashMap neumí
 * multithreading a kontrola unikátnosti jména musí proběhnout atomicky.
 */
public class ActiveHandlers {
    private static final long serialVersionUID = 1L;

    /** Místnost, do které je každý klient zařazen hned po připojení. */
    private static final String DEFAULT_ROOM = "public";

    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();

    /** Klienti s nastaveným jménem, klíčem je jméno. */
    private HashMap<String, SocketHandler> handlersByName = new HashMap<String, SocketHandler>();

    /**
     * sendMessageToRooms - Pošle zprávu všem klientům, kteří s odesílatelem sdílejí
     * alespoň jednu místnost, kromě odesílatele samotného. Kdo je s odesílatelem
     * ve více místnostech, dostane zprávu jen jednou.
     *
     * @param sender  - reference odesílatele
     * @param message - řetězec se zprávou
     * @return false, pokud odesílatel není v žádné místnosti
     */
    synchronized boolean sendMessageToRooms(SocketHandler sender, String message) {
        if (sender.rooms.isEmpty())
            return false;
        for (SocketHandler handler : activeHandlersSet)
            if (handler != sender && !Collections.disjoint(handler.rooms, sender.rooms))
                handler.send(message);
        return true;
    }

    /**
     * sendPrivateMessage - Pošle zprávu jedinému klientovi podle jeho jména.
     *
     * @param recipientName - jméno příjemce
     * @param message       - řetězec se zprávou
     * @return false, pokud klient s tímto jménem není připojen
     */
    synchronized boolean sendPrivateMessage(String recipientName, String message) {
        SocketHandler recipient = handlersByName.get(recipientName);
        if (recipient == null)
            return false;
        recipient.send(message);
        return true;
    }

    /**
     * setName nastaví klientovi jméno a uvolní to předchozí.
     *
     * @param handler - klient, který si jméno mění
     * @param name    - nové jméno
     * @return false, pokud jméno používá jiný klient (jméno zůstane beze změny)
     */
    synchronized boolean setName(SocketHandler handler, String name) {
        SocketHandler owner = handlersByName.get(name);
        if (owner != null && owner != handler)
            return false;
        if (handler.name != null)
            handlersByName.remove(handler.name);
        handler.name = name;
        handlersByName.put(name, handler);
        return true;
    }

    /** joinRoom přidá klienta do místnosti. Místnost existuje, dokud je v ní někdo. */
    synchronized void joinRoom(SocketHandler handler, String room) {
        handler.rooms.add(room);
    }

    /**
     * leaveRoom odebere klienta z místnosti.
     *
     * @return false, pokud klient v místnosti nebyl
     */
    synchronized boolean leaveRoom(SocketHandler handler, String room) {
        return handler.rooms.remove(room);
    }

    /** getRooms vrátí místnosti klienta ve formátu místnost1,místnost2,... */
    synchronized String getRooms(SocketHandler handler) {
        return String.join(",", handler.rooms);
    }

    /**
     * add přidá do množiny aktivních handlerů nový handler a zařadí ho do výchozí
     * místnosti.
     *
     * @param handler - reference na handler, který se má přidat.
     * @return true if the set did not already contain the specified element.
     */
    synchronized boolean add(SocketHandler handler) {
        handler.rooms.add(DEFAULT_ROOM);
        return activeHandlersSet.add(handler);
    }

    /**
     * remove odebere z množiny aktivních handlerů handler a uvolní jeho jméno.
     *
     * @param handler - reference na handler, který se má odstranit
     * @return true if the set contained the specified element.
     */
    synchronized boolean remove(SocketHandler handler) {
        if (handler.name != null)
            handlersByName.remove(handler.name);
        return activeHandlersSet.remove(handler);
    }
}
